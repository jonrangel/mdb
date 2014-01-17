/* mdb.h
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

#ifndef MDB_H
#define MDB_H


#include <bson.h>
#include <stddef.h>


BSON_BEGIN_DECLS


typedef struct _db_t db_t;
typedef struct _extent_t extent_t;
typedef struct _file_t file_t;
typedef struct _ns_t ns_t;
typedef struct _record_t record_t;


struct _file_t
{
   file_t  *next;
   int      fileno;
   int      fd;
   char    *map;
   size_t   maplen;
};


struct _db_t
{
   char   *dbpath;
   char   *name;
   file_t  nsfile;
   file_t *files;
   int     filescnt;
};


int  db_init        (db_t *db,
                     const char *dbpath,
                     const char *name);
int  db_namespaces  (db_t *db,
                     ns_t *ns);
void db_destroy     (db_t *db);


struct _extent_t
{
   db_t         *db;
   const char   *map;
   size_t        maplen;
   bson_int32_t  offset;
};


int extent_next    (extent_t *extent);
int extent_records (extent_t *extent,
                    record_t *record);


struct _record_t
{
   const char *map;
   off_t offset;
   bson_t bson;
};


int           record_next (record_t *record);
const bson_t *record_bson (record_t *record);


struct _ns_t
{
   db_t   *db;
   int     index;
   file_t *file;
};


int         ns_next        (ns_t *ns);
const char *ns_name        (const ns_t *ns);
int         ns_extents     (ns_t *ns,
                            extent_t *extent);


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


#define N_BUCKETS 19


#pragma pack(push, 1)
typedef struct {
   file_loc_t first_extent;
   file_loc_t last_extent;
   file_loc_t buckets[N_BUCKETS];
   struct {
      bson_int64_t datasize;
      bson_int64_t nrecords;
   } stats;
   bson_int32_t last_extent_size;
   bson_int32_t nindexes;
   /* some stuff is missing */
} ns_details_t;
#pragma pack(pop)


ns_details_t *
ns_get_details (ns_t *ns);


BSON_END_DECLS


#endif /* MDB_H */
