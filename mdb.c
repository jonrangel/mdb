/* mdb.c
 *
 * Copyright (C) 2013 10gen, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */


#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <stddef.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "mdb.h"


/*
 * Hello reader,
 *
 * This currently does NOT CHECK ENDIANNESS or any of that Jazz. It simply
 * reads the data as it is on disk. Also, it doesn't bring in all the features
 * of the disk layer since that would cause me to be more careful with the
 * following disk structures.
 *
 * This should serve, however, as a good explaination of the basic disk
 * format for MongoDB.
 *
 * Making something concurrently access this data should be possible since
 * this will only open files in O_RDONLY.
 *
 * The <dbname>.ns file is a bit tricky since it is actually a fixed size
 * (closed) hashtable. We just use the mmap of the file and hard code our
 * offsets for the NamespaceDetails (as it is in C++ server).
 *
 * You might notice that a lot of field types are signed integers. While
 * I'm not a fan of this, it seems that they are meant to be signed, so
 * we are in Rome it seems.
 *
 *   -- Christian Hergert
 */


#define EXTENT_MAGIC    BSON_UINT32_TO_LE(0x41424344)
#define NS_DETAILS_SIZE 496


#pragma pack(push, 1)
typedef struct {
   bson_int32_t fileno;
   bson_int32_t offset;
} file_loc_t;
#pragma pack(pop)


BSON_STATIC_ASSERT(sizeof(file_loc_t) == 8);


#pragma pack(push, 1)
typedef struct {
   bson_int32_t version;
   bson_int32_t version_minor;
   bson_int32_t file_length;
   file_loc_t   unused;
   bson_int32_t unused_length;
   char         reserved[8192 - 4*4 - 8];
   char         data[4];
} file_header_t;
#pragma pack(pop)


BSON_STATIC_ASSERT(sizeof(file_header_t)-4 == 8192);


#pragma pack(push, 1)
typedef struct {
   bson_int32_t magic;
   file_loc_t   my_loc;
   file_loc_t   next;
   file_loc_t   prev;
   char         namespace[128];
   bson_int32_t length;
   file_loc_t   first_record;
   file_loc_t   last_record;
} extent_header_t;
#pragma pack(pop)


BSON_STATIC_ASSERT(offsetof(extent_header_t, first_record) == 160);
BSON_STATIC_ASSERT(sizeof(extent_header_t) == 176);


#pragma pack(push, 1)
typedef struct {
   bson_int32_t length;
   bson_int32_t extent_offset;
   bson_int32_t next_offset;
   bson_int32_t prev_offset;
   char         data[4];
} record_header_t;
#pragma pack(pop)


#pragma pack(push, 1)
typedef struct {
   bson_int32_t hash;
   char key[128];
   char details[NS_DETAILS_SIZE];
} ns_hash_node_t;
#pragma pack(pop)


#pragma pack(push, 1)
typedef struct {
   file_loc_t first_extent;
   file_loc_t last_extent;
   file_loc_t buckets[19];
   struct {
      bson_int64_t datasize;
      bson_int64_t nrecords;
   } stats;
   bson_int32_t last_extent_size;
   bson_int32_t nindexes;
   /* some stuff is missing */
} ns_details_t;
#pragma pack(pop)


/*
 *--------------------------------------------------------------------------
 *
 * file_init --
 *
 *       Initialize a new file_t using the path provided. The file will be
 *       opened read-only. The file descriptor will be used to mmap() the
 *       entire contents of the file.
 *
 *       @file is the file to initialize.
 *       @fileno is the file number, or -1 for the ns file.
 *       @path is the path to the file.
 *
 * Returns:
 *       0 on success -- otherwise -1 and errno is set.
 *
 * Side effects:
 *       file is initialized if successful.
 *
 *--------------------------------------------------------------------------
 */

static int
file_init (file_t *file,     /* OUT */
           int fileno,       /* IN */
           const char *path) /* IN */
{
   struct stat st;
   void *map;
   int fd;

   if (!file || !path) {
      errno = EINVAL;
      return -1;
   }

   memset(file, 0, sizeof *file);

   if (!!access(path, R_OK)) {
      return -1;
   }

   if (!!stat(path, &st)) {
      return -1;
   }

   if (-1 == (fd = open(path, O_RDONLY))) {
      return -1;
   }

   if (!(map = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fd, 0))) {
      close(fd);
      return -1;
   }

   file->next = NULL;
   file->fileno = fileno;
   file->fd = fd;
   file->map = map;
   file->maplen = st.st_size;

   return 0;
}


/*
 *--------------------------------------------------------------------------
 *
 * file_close --
 *
 *       Closes the file descriptors and mmap() regions for @file.
 *
 * Returns:
 *       -1 on close failure.
 *
 * Side effects:
 *       file is destroyed and all values unset.
 *
 *--------------------------------------------------------------------------
 */

static int
file_close (file_t *file) /* IN */
{
   int fd;

   if (!file) {
      errno = EINVAL;
      return -1;
   }

   fd = file->fd;

   if (file->map) {
      munmap(file->map, file->maplen);
   }
   file->fd = -1;
   file->map = NULL;
   file->maplen = 0;

   return close(fd);
}


/*
 *--------------------------------------------------------------------------
 *
 * db_init --
 *
 *       Initializes a new db_t using the dbpath directory on disk and
 *       the name of the database. The dbpath and name are combined to
 *       load the individual database files such as dbpath/name.ns.
 *
 * Returns:
 *       0 on success -- otherwise -1 and errno is set.
 *
 * Side effects:
 *       db is initialized.
 *
 *--------------------------------------------------------------------------
 */

int
db_init (db_t *db,            /* OUT */
         const char *dbpath,  /* IN */
         const char *name)    /* IN */
{
   file_t *files = NULL;
   file_t *last = NULL;
   file_t *iter;
   char *path;
   int fileno = 0;

   if (!db || !dbpath || !name) {
      errno = EINVAL;
      return -1;
   }

   memset(db, 0, sizeof *db);

   /*
    * Try to load our namespace file.
    */
   path = bson_strdup_printf("%s/%s.ns", dbpath, name);
   if (!!file_init(&db->nsfile, -1, path)) {
      bson_free(path);
      return -1;
   }
   bson_free(path);

   /*
    * Try to load all of our numbered data files. Calling file_init() will
    * result in the files being fully mmap()'d.
    */
   for (;; last = &files[fileno++]) {
      path = bson_strdup_printf("%s/%s.%d", dbpath, name, fileno);
      if (!!access(path, R_OK)) {
         bson_free(path);
         break;
      }

      files = bson_realloc(files, (fileno + 1) * sizeof(file_t));
      if (!!file_init(&files[fileno], fileno, path)) {
         bson_free(path);
         goto failure;
      }

      if (last) {
         last->next = &files[fileno];
      }
   }

   db->dbpath = strdup(dbpath);
   db->name = strdup(name);
   db->files = files;
   db->filescnt = fileno;

   return 0;

failure:
   for (iter = files; iter; iter = iter->next) {
      file_close(iter);
   }

   bson_free(files);

   return -1;
}


/*
 *--------------------------------------------------------------------------
 *
 * db_namespaces --
 *
 *       Fetches the first of a series of namespaces. Use ns_next() to
 *       move to the next namespace. You can fetch the name of the
 *       namespace with ns_name().
 *
 *       A namespace contains extents, which can be fetched with
 *       ns_next();
 *
 * Returns:
 *       0 on success -- otherwise -1 and errno is set.
 *
 * Side effects:
 *       ns is initialized.
 *
 *--------------------------------------------------------------------------
 */

int
db_namespaces (db_t *db,   /* IN */
               ns_t *ns)   /* OUT */
{
   if (!db || !ns) {
      errno = EINVAL;
      return -1;
   }

   memset(ns, 0, sizeof *ns);

   ns->db = db;
   ns->file = &db->nsfile;
   ns->index = -1;

   return ns_next(ns);
}


/*
 *--------------------------------------------------------------------------
 *
 * ns_hash_node --
 *
 *       Fetch the ns_hash_node_t for the current namespace entry.
 *
 * Returns:
 *       ns_hash_node_t* if sucessful -- otherwise NULL and errno is set.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

static ns_hash_node_t *
ns_hash_node (const ns_t *ns)
{
   ns_hash_node_t *node;

   if (!ns || !ns->file || ns->index < 0) {
      errno = EINVAL;
      return NULL;
   }

   node = (ns_hash_node_t *)ns->file->map;
   node += ns->index;

   return node;
}


/*
 *--------------------------------------------------------------------------
 *
 * ns_name --
 *
 *       Fetch the name of the namespace.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

const char *
ns_name (const ns_t *ns)
{
   ns_hash_node_t *node;

   if (!ns) {
      errno = EINVAL;
      return NULL;
   }

   node = ns_hash_node(ns);

   return node->key;
}


/*
 *--------------------------------------------------------------------------
 *
 * ns_next --
 *
 *       Move to the next namespace entry in the "dbname.ns" file.
 *
 * Returns:
 *       0 on success -- otherwise -1 and errno is set.
 *
 * Side effects:
 *       ns is updated to reflect the next namespace.
 *
 *--------------------------------------------------------------------------
 */

int
ns_next (ns_t *ns)
{
   ns_hash_node_t *node;

   if (!ns) {
      errno = EINVAL;
      return -1;
   }

next:
   ns->index++;

   node = ns_hash_node(ns);

   if ((((char *)node) + sizeof *node) >
       (ns->file->map + ns->file->maplen)) {
      ns->index = -1;
      return -1;
   }

   if (!node->key[0]) {
      goto next;
   }

   return 0;
}


/*
 *--------------------------------------------------------------------------
 *
 * ns_extents --
 *
 *       Fetches the first extent for the current namespace. You can move
 *       to the next extent with ns_next().
 *
 * Returns:
 *       0 on success -- otherwise -1 and errno is set.
 *
 * Side effects:
 *       extent is initialized.
 *
 *--------------------------------------------------------------------------
 */

int
ns_extents (ns_t *ns,         /* IN */
            extent_t *extent) /* OUT */
{
   const bson_int32_t magic = EXTENT_MAGIC;
   ns_hash_node_t *node;
   ns_details_t *details;
   file_loc_t *loc;

   if (!ns || !ns->db || !extent) {
      errno = EINVAL;
      return -1;
   }

   memset(extent, 0, sizeof *extent);

   if (!(node = ns_hash_node(ns))) {
      errno = ENOENT;
      return -1;
   }

   details = (ns_details_t *)node->details;
   loc = &details->first_extent;

   if ((loc->fileno >= ns->db->filescnt) ||
       (loc->offset >= ns->db->files[loc->fileno].maplen)) {
      errno = ENOENT;
      return -1;
   }

   extent->db = ns->db;
   extent->map = ns->db->files[loc->fileno].map;
   extent->maplen = ns->db->files[loc->fileno].maplen;
   extent->offset = loc->offset;

   if (!!memcmp(extent->map + extent->offset, &magic, sizeof magic)) {
      memset(extent, 0, sizeof *extent);
      errno = EBADF;
      return -1;
   }

   return 0;
}


/*
 *--------------------------------------------------------------------------
 *
 * extent_next --
 *
 *       Advances the extent_t to point at the next extent in the on-disk
 *       linked list of extents.
 *
 * Returns:
 *       0 on success -- otherwise -1 and errno is set.
 *
 * Side effects:
 *       extent is updated to point at the next extent.
 *
 *--------------------------------------------------------------------------
 */

int
extent_next (extent_t *extent) /* IN */
{
   extent_header_t *ehdr;
   file_loc_t next;

   if (!extent) {
      errno = EINVAL;
      return -1;
   }

   ehdr = (extent_header_t *)(extent->map + extent->offset);
   next = ehdr->next;

   if (next.fileno == -1) {
      errno = ENOENT;
      return -1;
   }

   extent->map = extent->db->files[next.fileno].map;
   extent->maplen = extent->db->files[next.fileno].maplen;
   extent->offset = next.offset;

   return 0;
}


/*
 *--------------------------------------------------------------------------
 *
 * extent_records --
 *
 *       Fetches the first record in an extent. You can move to the next
 *       record using record_next().
 *
 * Returns:
 *       0 on success -- otherwise -1 and errno is set.
 *
 * Side effects:
 *       record is initialized.
 *
 *--------------------------------------------------------------------------
 */

int
extent_records (extent_t *extent,   /* IN */
                record_t *record)   /* OUT */
{
   extent_header_t *ehdr;

   if (!extent || !record) {
      errno = EINVAL;
      return -1;
   }

   memset(record, 0, sizeof *record);

   ehdr = (extent_header_t *)(extent->map + extent->offset);
   if (ehdr->first_record.offset < 0) {
      errno = EBADF;
      return -1;
   }

   record->map = extent->map;
   record->offset = ehdr->first_record.offset;

   return 0;
}


/*
 *--------------------------------------------------------------------------
 *
 * record_next --
 *
 *       Move the record_t to the next record in the extent.
 *
 * Returns:
 *       0 on success -- otherwise -1 and errno is set.
 *
 * Side effects:
 *       record is updated to point to new record.
 *
 *--------------------------------------------------------------------------
 */

int
record_next (record_t *record) /* IN/OUT */
{
   record_header_t *rhdr;

   if (!record) {
      errno = EINVAL;
      return -1;
   }


   rhdr = (record_header_t *)(record->map + record->offset);
   if (rhdr->next_offset < 0) {
      errno = ENOENT;
      return -1;
   }

   record->offset = rhdr->next_offset;

   return 0;
}


/*
 *--------------------------------------------------------------------------
 *
 * record_bson --
 *
 *       Get the BSON document associated with the record. You may iterate
 *       through the document using bson_iter_*() functions.
 *
 * Returns:
 *       A const bson_t* on success -- otherwise NULL and errno is set.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

const bson_t *
record_bson (record_t *record)
{
   record_header_t *rhdr;
   bson_int32_t len;

   if (!record) {
      errno = EINVAL;
      return NULL;
   }

   rhdr = (record_header_t *)(record->map + record->offset);

   memcpy(&len, rhdr->data, 4);
   BSON_ASSERT(len <= rhdr->length);

   if (bson_init_static(&record->bson, (bson_uint8_t *)rhdr->data, len)) {
      return &record->bson;
   }

   return NULL;
}


/*
 *--------------------------------------------------------------------------
 *
 * db_destroy --
 *
 *       Release resources associated with the structure to the system
 *       and close any open files.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       Everything.
 *
 *--------------------------------------------------------------------------
 */

void
db_destroy (db_t *db)
{
   bson_return_if_fail(db);

   /*
    * TODO: Free up everything and close files.
    */
}
