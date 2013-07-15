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
   const char *map;
   size_t      maplen;
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
   int index;
   file_t *file;
};


int         ns_next        (ns_t *ns);
const char *ns_name        (const ns_t *ns);
int         ns_extents     (ns_t *ns,
                            extent_t *extent);


BSON_END_DECLS


#endif /* MDB_H */
