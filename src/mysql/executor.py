import logging
import abc
import json
import time
import mysql.connector
import uuid

from src.executor import AbstractBenchmarkRunnable

logger = logging.getLogger(__name__)


class AbstractMySQLRunnable(AbstractBenchmarkRunnable, abc.ABC):
    def connect(self, database=None):
        return mysql.connector.connect(
            autocommit=True,
            host=self.config['hostname'],
            user=self.config['username'],
            password=self.config['password'],
            database=database if database is not None else self.config['database']
        )

    @staticmethod
    def _execute_multiple_statements(cursor, statement):
        # Note: this does not consume any results! Multiple SELECTs must be issued in different execute_sql calls.
        lean_statement = ' '.join(statement.split())
        split_statement = [s + ';' for s in lean_statement.split(';') if len(s) > 0]
        for single_statement in split_statement:
            cursor.execute(single_statement)

    def enable_profiling(self, cursor):
        self._execute_multiple_statements(cursor, """
            UPDATE performance_schema.setup_instruments
            SET ENABLED = 'YES', TIMED = 'YES'
            WHERE NAME LIKE '%statement/%';
            
            UPDATE performance_schema.setup_instruments
            SET ENABLED = 'YES', TIMED = 'YES'
            WHERE NAME LIKE '%stage/%';
            
            UPDATE performance_schema.setup_consumers
            SET ENABLED = 'YES'
            WHERE NAME LIKE '%events_statements_%';
            
            UPDATE performance_schema.setup_consumers
            SET ENABLED = 'YES'
            WHERE NAME LIKE '%events_stages_%';
        """)

    def execute_importjson(self, file_path, table, column):
        import_command = f"""
            util.import_json ("{file_path}", {{ 
                "schema": "{self.config['database']}",
                "table": "{table}",
                "tableColumn": "{column}"
            }});
        """.strip()
        shell_command = self.config['shellCommand'][:-1] + [
            self.config['shellCommand'][-1] + ' ' + ' '.join(f"""
                --user={self.config['username']}
                --password={self.config['password']}
                --json=raw
                --python
                --execute '{import_command}'
            """.split())
        ]

        raw_execution_output = self.call_subprocess(shell_command, is_log=False)
        parsed_execution_output = [
            json.loads(r) for r in raw_execution_output.split('\n')  # Remove those pesky warnings. :-)
            if len(r) > 0 and r[0] == '{' and 'Using a password on the command line interface can be insecure' not in r
        ]

        return {'raw_output': raw_execution_output, 'parsed_output': parsed_execution_output}

    def execute_sql(self, cursor, statement):
        event_id = str(uuid.uuid4())
        event_tag = f'/* EVENT UUID: {event_id} */ '
        tag_statement = ' '.join([event_tag + ' '.join(s.split()) + ';' for s in statement.split(';')[:-1]])
        try:
            self._execute_multiple_statements(cursor, tag_statement)
            statement_results = [r for r in cursor] if cursor.with_rows else None

            cursor.execute(f"""
                SELECT TRUNCATE(TIMER_WAIT/1000000000000,6) AS duration, SQL_TEXT
                FROM performance_schema.events_statements_history_long
                WHERE SQL_TEXT LIKE '%{event_id}%'; 
            """)
            profile_results = [(float(r[0]), r[1]) for r in cursor.fetchall()]

            return {'event_id': event_id, 'statement': tag_statement, 'statement_results': statement_results,
                    'profile_results': profile_results}

        except mysql.connector.Error as e:
            return {'event_id': event_id, 'statement': tag_statement, 'error': str(e)}

    def restart_db(self):
        super(AbstractMySQLRunnable, self).restart_db()
        while True:
            time.sleep(10)
            try:
                connection = self.connect()
                connection.ping()
                break
            except mysql.connector.InterfaceError:
                logger.warning(f'Instance is not currently online. Trying again in 10 seconds...')
