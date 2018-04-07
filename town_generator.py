import random

_50k = 50000
_100k = 100000
_1M = 1000000
_10M = 10000000


total_towns = _10M
# total_connections = 10 * total_towns
connections_from_town = 20
total_types = 10


def write_towns():
    with open('town.csv', 'w') as f:
        for x in range(1, total_towns):
            f.write('{x};town{x}\n'.format(x=x))


def write_towns_with_types():
    with open('town_with_types.csv', 'w') as f:
        for x in range(1, total_towns):
            town_type = x % total_types
            f.write('{x};town{x};{town_type}\n'.format(x=x, town_type=town_type))


def write_sql_connections():
    with open('sql_town_connections.csv', 'w') as f:
        for x in range(1, total_towns):
            if not x % 1000:
                print('processed', x)
            for y in range(1, connections_from_town + 1):
                f.write('{from_nid};{to_nid}\n'.format(
                    from_nid=x,
                    to_nid=random.randint(1, total_towns)
                ))


if __name__ == '__main__':
    write_towns_with_types()
    # write_towns()
    # write_sql_connections()
