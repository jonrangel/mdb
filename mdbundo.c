#include <assert.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "mdb.h"


static void
usage (void)
{
   fprintf (stderr, "usage: mdbundo DBPATH DBNAME COLNAME\n");
}


static int
fixup_bson (char *base,
            int   maxlen)
{
   bson_iter_t iter;
   bson_t b;
   int last_offset = 0;

   memcpy (base, &maxlen, 4);
   base [maxlen - 1] = '\0';

   if (!bson_init_static (&b, (bson_uint8_t *)base, maxlen)) {
      assert (FALSE);
   }

   if (!bson_iter_init (&iter, &b)) {
      assert (FALSE);
   }

   while (bson_iter_next (&iter)) {
      last_offset = iter.next_off;
   }

   if (last_offset) {
      memset (base + last_offset, 0, maxlen - last_offset);
      maxlen = last_offset + 1;
      memcpy (base, &maxlen, 4);
   }

   return 1;
}


static record_header_t *
get_record_at_loc (db_t       *db,
                   file_loc_t *loc)
{
   char *base;

   assert (loc->fileno != -1);

   base = db->files [loc->fileno].map;
   base = base + loc->offset;
   return (record_header_t *)base;
}


static int
get_bson_at_loc (db_t       *db,
                 file_loc_t *loc,
                 bson_t     *b)
{
   record_header_t *rec;
   size_t off;
   int len;

   assert (loc->fileno != -1);

   rec = get_record_at_loc (db, loc);

   len = rec->length - 16;

   if (!fixup_bson (rec->data, len)) {
      return 0;
   }

   memcpy (&len, rec->data, 4);

   if (!bson_init_static (b, (bson_uint8_t *)rec->data, len)) {
      return 0;
   }

   if (bson_validate (b, BSON_VALIDATE_NONE, &off)) {
      return 1;
   }

   return 0;
}


static void
mdbundo (ns_t *ns)
{
   ns_details_t *details;
   record_header_t *record;
   file_loc_t loc;
   bson_t b;
   int i;

   details = ns_get_details (ns);

   for (i = 0; i < N_BUCKETS; i++) {
      if (details->buckets [i].fileno == -1) {
         continue;
      }

      loc.fileno = details->buckets [i].fileno;
      loc.offset = details->buckets [i].offset;

      record = get_record_at_loc (ns->db, &loc);

      do {
         if (get_bson_at_loc (ns->db, &loc, &b)) {
            const bson_uint8_t *data;
            size_t len;

            data = bson_get_data (&b);
            len = b.len;

            if (len != write (STDOUT_FILENO, data, len)) {
               assert (FALSE);
            }
         } else {
            printf ("Failed to load document.\n");
         }

         loc.fileno = record->next_offset;
         loc.offset = record->prev_offset;

         if (loc.fileno == -1 || loc.offset == 0) {
            break;
         }

         /* next fileno is overriden from old prev field. */
      } while ((record = get_record_at_loc (ns->db, &loc)));
   }
}


int
main (int   argc,
      char *argv[])
{
   const char *dbpath;
   const char *dbname;
   const char *colname;
   char dotname [128];
   db_t db;
   ns_t ns;

   if (argc != 4) {
      usage ();
      return EXIT_FAILURE;
   }

   dbpath = argv [1];
   dbname = argv [2];
   colname = argv [3];

   snprintf (dotname, sizeof dotname, "%s.%s", dbname, colname);
   dotname [sizeof dotname - 1] = '\0';

   if (0 != db_init (&db, dbpath, dbname)) {
      perror ("Failed to load database");
      return EXIT_FAILURE;
   }

   if (0 != db_namespaces (&db, &ns)) {
      perror ("Failed to locate namespaces");
      return EXIT_FAILURE;
   }

   do {
      if (0 == strcmp (dotname, ns_name (&ns))) {
         mdbundo (&ns);
      }
   } while (0 == ns_next (&ns));

   return EXIT_SUCCESS;
}
