create table svcdb.log_event (id SERIAL primary key not null, metric char(50), log_date timestamp, data json)
create table svcdb.topic_offsets (
  topic character varying(255),
  part integer,
  off bigint,
  unique (topic, part)
);
-- manual insert offset for first time -> need to be improved
insert into svcdb.topic_offsets values ('prod-pageview', 0, 0);
insert into svcdb.topic_offsets values ('prod-pageview', 1, 0);
insert into svcdb.topic_offsets values ('prod-pageview', 2, 0);
insert into svcdb.topic_offsets values ('prod-pageview', 3, 0);

insert into svcdb.topic_offsets values ('prod-click', 0, 0);
insert into svcdb.topic_offsets values ('prod-click', 1, 0);
insert into svcdb.topic_offsets values ('prod-click', 2, 0);
insert into svcdb.topic_offsets values ('prod-click', 3, 0);

insert into svcdb.topic_offsets values ('prod-order', 0, 0);
insert into svcdb.topic_offsets values ('prod-order', 1, 0);
insert into svcdb.topic_offsets values ('prod-order', 2, 0);
insert into svcdb.topic_offsets values ('prod-order', 3, 0);

-- auto create partition on insert
-- CREATE FUNCTION ADD PARTITON
CREATE OR REPLACE FUNCTION create_partition_and_insert()
RETURNS trigger AS
$BODY$
DECLARE
partition VARCHAR(25);
BEGIN
partition := TG_RELNAME || '_' || to_char(NEW.log_date, 'yyyy_MM_dd');
IF NOT EXISTS(SELECT relname FROM pg_class WHERE relname=partition) THEN
RAISE NOTICE 'A partition has been created %',PARTITION;
EXECUTE 'CREATE TABLE svcdb.' || PARTITION || ' (check (log_date >= ''' || date_trunc('day', NEW.log_date) || ''' AND log_date < ''' || date_trunc('day', NEW.log_date) + '1 days' ||''')) INHERITS (svcdb.' || TG_RELNAME || ');';
END IF;
EXECUTE 'INSERT INTO svcdb.' || partition || ' SELECT(svcdb.' || TG_RELNAME || ' ' || quote_literal(NEW) || ').*;';
RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql VOLATILE
COST 100;

--CREATE ON INSERT TRIGGER
CREATE TRIGGER log_event_insert_trigger
BEFORE INSERT ON svcdb.log_event
FOR EACH ROW EXECUTE PROCEDURE create_partition_and_insert();