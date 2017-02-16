from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()

# Create Keyspace if it does not exist yet
session.execute(
    """
    CREATE KEYSPACE IF NOT EXISTS wiki WITH REPLICATION = {
        'class' : 'SimpleStrategy',
        'replication_factor' : 3
    }
    """
)

# Create tables for hourly, daily, and trending views
session.set_keyspace('wiki')

session.execute(
    """
    CREATE TABLE daily (
        title varchar,
        ymdh varchar,
        vcount int,
    PRIMARY KEY(ymdh,title)
    );
    """
)


session.execute(
    """
    CREATE TABLE wiki.hourly (
        title varchar,
        ymdh varchar,
        vcount int,
    PRIMARY KEY (title, ymdh) );
    """
)

session.execute(
    """
    CREATE TABLE wiki.trending (
        title varchar,
        ymdh varchar,
        vcount int,
    PRIMARY KEY(title,ymdh)
    );
    """
)


session.execute(
    """
    CREATE TABLE wiki.test (
        title varchar,
        ymdh varchar,
        vcount int,
    PRIMARY KEY (title, ymdh) );
    """
)

session.execute(
    """
    CREATE KEYSPACE IF NOT EXISTS graph WITH REPLICATION = {
        'class' : 'SimpleStrategy',
        'replication_factor' : 3
    }
    """
)

# create graph
session.set_keyspace('graph')

session.execute(
    """
    CREATE TABLE graph.g1 (
        pgfrom varchar,
        pgto varchar,
        pgtoto list<varchar>,
    PRIMARY KEY (pgfrom, pgto) );
    """
)
