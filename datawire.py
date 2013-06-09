import sqlaload as sl
from datastringer import DataStringer

from common import *
from dump import generate_all

log = logging.getLogger('dump')


def submit_all():
    engine = db_connect()
    spending = sl.get_table(engine, 'spending')
    stringer = DataStringer(service='ukspending',
                            event='transactions')
    log.info("Submitting frames to datawire...")
    for i, row in enumerate(generate_all()):
        event_at = row.get('DateFormatted')
        if event_at:
            event_at = datetime.datetime.strptime(event_at, '%Y-%m-%d')
        stringer.submit(row,
                        event_at=event_at,
                        source_url=row.get('SourceURL'))
    data = {'datawire_submitted': True}
    sl.update(engine, spending, row)


if __name__ == '__main__':
    submit_all()

