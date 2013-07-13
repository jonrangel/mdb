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
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "mdb.h"


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
 *       0 on success; otherwise -1 and errno is set.
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
 *       0 on success; otherwise -1 and errno is set.
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
 *       0 on success; otherwise -1 and errno is set.
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

   return 0;
}
