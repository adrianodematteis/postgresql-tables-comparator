from .red_shift_connection import Connection


def compare_tables(config_file, connection=None):
    if connection is None:
        connection = Connection(config_file['connection'])

    report = ''

    for table_pair in config_file['tablePairs']:
        old_schema = table_pair['oldTable']['schema']
        old_name = table_pair['oldTable']['tableName']
        new_schema = table_pair['newTable']['schema']
        new_name = table_pair['newTable']['tableName']

        old_columns = connection.table_columns(old_schema, old_name)

        if old_columns == connection.table_columns(new_schema, new_name):
            stt = "Tables {}.{} and {}.{} have the same columns".format(old_schema, old_name, new_schema, new_name)

            sql = "select count(*) from {}.{} o full outer join {}.{} n using ({}) where o.{} is null or n.{} is null" \
                .format(old_schema, old_name, new_schema, new_name, ', '.join(old_columns), old_columns[0],
                        old_columns[0])
            except_count = connection.execute_and_fetch(sql)[0]
            exc_stt = "\tNumber of different rows (sum of the rows that are in the old table but not in the new one " \
                      "and viceversa): {} \n\n".format(except_count)

            report = '\n'.join([report, stt, exc_stt])

        else:
            stt = "Tables {}.{} and {}.{} do not have the same columns \n\n" \
                .format(old_schema, old_name, new_schema, new_name)
            report = report.join(stt)

    return report

