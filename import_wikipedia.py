#!/usr/bin/env python

import argparse
import logging
import xml.sax
import bz2
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
import mwparserfromhell
import psycopg2
import re
from psycopg2.extras import DictCursor, execute_batch
import time
import sys

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CAT_PREFIX = 'Category:'
INFOBOX_PREFIX = 'infobox '

RE_GENERAL = re.compile('(.+?)(\ (in|of|by)\ )(.+)')


def setup_db(connection_string):
    logger.info('Setting up database connection...')
    try:
        conn = psycopg2.connect(connection_string, cursor_factory=DictCursor)
        logger.info('Database connection established.')
    except psycopg2.Error as e:
        if e.pgcode == '3D000':  # database does not exist
            logger.info('Database does not exist, creating it...')
            conn = psycopg2.connect(
                connection_string.replace('dbname=wikipedia_imports', ''),
                cursor_factory=DictCursor
            )
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute('CREATE DATABASE wikipedia_imports')
            conn.close()
            conn = psycopg2.connect(connection_string, cursor_factory=DictCursor)
        else:
            logger.error(f'Failed to establish database connection: {e}')
            return None, None

    cursor = conn.cursor()
    logger.info('Creating or dropping table wikipedia...')
    try:
        cursor.execute('DROP TABLE IF EXISTS wikipedia')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS wikipedia (
                title TEXT PRIMARY KEY,
                wiki_id INTEGER,
                infobox TEXT,
                wikitext TEXT,
                templates TEXT[] NOT NULL DEFAULT '{}',
                categories TEXT[] NOT NULL DEFAULT '{}'
            )
        ''')
        cursor.execute('''
            ALTER TABLE wikipedia
            ADD CONSTRAINT unique_title_wiki_id UNIQUE (title, wiki_id)
        ''')
        conn.commit()
        logger.info('Table wikipedia created.')
    except psycopg2.Error as e:
        logger.error(f'Failed to create table wikipedia: {e}')
        return None, None

    logger.info('Creating indexes...')
    try:
        cursor.execute('CREATE INDEX IF NOT EXISTS wikipedia_infobox ON wikipedia(infobox)')
        cursor.execute('CREATE INDEX IF NOT EXISTS wikipedia_templates ON wikipedia USING gin(templates)')
        cursor.execute('CREATE INDEX IF NOT EXISTS wikipedia_categories ON wikipedia USING gin(categories)')
        conn.commit()
        logger.info('Indexes created.')
    except psycopg2.Error as e:
        logger.error(f'Failed to create indexes: {e}')
        return None, None

    return conn, cursor


def make_tags(iterable):
    logger.debug('Converting to tags...')
    return list(set(x.strip().lower() for x in iterable if x and len(x) < 256))


def strip_template_name(name):
    logger.debug('Stripping template name...')
    return name.strip_code().strip()


class WikiXmlHandler(xml.sax.handler.ContentHandler):
    def __init__(self, cursor, record_limit, batch_size=1000, max_workers=4):
        xml.sax.handler.ContentHandler.__init__(self)
        self._db_cursor = cursor
        self._count = 0
        self._processed_total = 0
        self._skipped_redirect = 0
        self._skipped_obsolete = 0
        self._total = 0
        self._record_limit = record_limit
        self._batch_size = batch_size
        self._batch = []
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._futures = set()
        self._start_time = time.time()
        self._last_update_time = time.time()
        self._rows_per_second = 0
        self._estimated_time_remaining = 0
        self.reset()

    def reset(self):
        logger.debug('Resetting handler...')
        self._buffer = []
        self._state = None
        self._values = {}

    def startElement(self, name, attrs):
        logger.debug('Start element: %s' % name)
        if name in ('title', 'text', 'id'):
            self._state = name

    def endElement(self, name):
        logger.debug('End element: %s' % name)
        if name == self._state:
            self._values[name] = ''.join(self._buffer)
            self._state = None
            self._buffer = []

        if name == 'page':
            self._total += 1
            self._futures.add(self._executor.submit(self.process_page, self._values.copy()))
            self.reset()

            # Check futures' results and add to batch
            done, _ = wait(self._futures, timeout=0.01, return_when=FIRST_COMPLETED)
            for future in done:
                try:
                    result = future.result()
                    if result:
                        if result == "skipped for redirects":
                            self._skipped_redirect += 1
                        elif result == "skipped for obsolete":
                            self._skipped_obsolete += 1
                        else:
                            self._batch.extend(result)
                            for entry in result:
                                logger.info(f'Inserting page title: {entry[0]}')
                            self._count += len(result)
                    self._futures.remove(future)
                except Exception as e:
                    logger.error(f"Error processing page: {e}")

            if len(self._batch) >= self._batch_size:
                self.insert_batch()
                
            self._processed_total += 1
            current_time = time.time()
            if current_time - self._last_update_time > 1:
                self._rows_per_second = self._processed_total / (current_time - self._start_time)
                if self._record_limit:
                    self._estimated_time_remaining = (self._record_limit - self._processed_total) / self._rows_per_second
                else:
                    self._estimated_time_remaining = (self._total - self._processed_total) / self._rows_per_second if self._total > self._processed_total else 0
                logger.info(f'Total pages: {self._total}. Inserted: {self._count}. Skipped: [redirects - {self._skipped_redirect}] [obsolete - {self._skipped_obsolete}]. Rows per second: {self._rows_per_second:.2f}. Estimated time remaining: {self._estimated_time_remaining / 60:.2f} minutes.')
                self._last_update_time = current_time

            if self._record_limit and self._processed_total >= self._record_limit:
                logger.info(f'Reached limit of {self._record_limit} processed records.')
                self._record_limit = 0  # Avoid further insertions if the limit is reached
            
    def characters(self, content):
        logger.debug('Characters: %s' % content)
        if self._state:
            self._buffer.append(content)

    def process_page(self, values):
        try:
            logger.debug('Parsing wikitext...')
            wikicode = mwparserfromhell.parse(values['text'])
            
            # Check for redirects and deprecated pages
            if any(values['text'].strip().lower().startswith(directive) for directive in ['#redirect', '#redirect', '#redirect']):
                logger.info(f'Skipping redirect page: {values["title"]}')
                return "skipped for redirects"
            
            if 'This page is obsolete!' in values['text']:
                logger.info(f'Skipping obsolete page: {values["title"]}')
                return "skipped for obsolete"
            
            template_dict = OrderedDict(
                (strip_template_name(template.name), template) for template in wikicode.filter_templates()
            )
                
            logger.debug('Making tags...')
            templates = make_tags(template_dict.keys())
            infobox = None
            for template in templates:
                if template.startswith(INFOBOX_PREFIX):
                    infobox = template[len(INFOBOX_PREFIX) :]
                    break
            categories = make_tags(
                l.title[len(CAT_PREFIX) :] for l in wikicode.filter_wikilinks() if l.title.startswith(CAT_PREFIX)
            )
                
            if 'obsolete list of encyclopedia topics' in categories:
                logger.info(f'Skipping page with obsolete category: {values["title"]}')
                return "skipped for obsolete"

            to_insert = (
                values['title'],
                int(values['id']),
                infobox,
                values['text'],
                templates,
                categories,
            )

            return [to_insert]

        except mwparserfromhell.parser.ParserError as e:
            logger.error(f'MWParser error: {e}')
            return None

    def insert_batch(self):
        if not self._batch:
            return
        sql = (
            "INSERT INTO wikipedia "
            "(title, wiki_id, infobox, wikitext, templates, categories) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (title, wiki_id) DO UPDATE "
            "SET infobox = EXCLUDED.infobox, wikitext = EXCLUDED.wikitext, templates = EXCLUDED.templates, categories = EXCLUDED.categories"
        )
        execute_batch(self._db_cursor, sql, self._batch)
        self._batch.clear()


def main(dump, cursor, record_limit):
    logger.info('Starting import...')
    handler = WikiXmlHandler(cursor, record_limit)
    try:
        with open(dump, 'rb') as file:
            file_data = bz2.BZ2File(file)
            parser = xml.sax.make_parser()
            parser.setContentHandler(handler)
            for line in file_data:
                parser.feed(line.decode('utf-8'))
                if record_limit and handler._processed_total >= record_limit:
                    break

    except KeyboardInterrupt:
        logger.warning('KeyboardInterrupt received. Committing current records to database...')
    finally:
        logger.info(f'Final statistics - Total pages processed: {handler._total}, Inserted: {handler._count}, Skipped: [redirects - {handler._skipped_redirect}] [obsolete - {handler._skipped_obsolete}]')
        if handler._batch:
            handler.insert_batch()  # Insert the remaining records in the batch
        conn.commit()
        cursor.close()
        conn.close()
        handler._executor.shutdown()

    logger.info('Import complete.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import wikipedia into postgress')
    parser.add_argument('--postgres', type=str, help='postgres connection string')
    parser.add_argument('--record_limit', type=int, default=0, help='if larger than 0, import only so many records')
    parser.add_argument('dump', type=str, help='BZipped wikipedia dump')

    args = parser.parse_args()
    conn, cursor = setup_db(args.postgres)

    main(args.dump, cursor, args.record_limit)

    logger.info('Database connection closed.')
