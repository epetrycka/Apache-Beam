import apache_beam as beam

def parse_and_split(record):
    return record.split(',')

def filter_accounts(record):
    return record[3] == 'Accounts'


p1 = beam.Pipeline() #Creating and giving pipeline a name
attendance_count = (
    p1
    | beam.io.ReadFromText('./data/dept_data.txt')
    | 'Parse and split' >> beam.Map(parse_and_split)
    | 'Filter Accounts' >> beam.Filter(filter_accounts)
    | beam.Map(lambda record: (record[1], 1))
    | beam.CombinePerKey(sum)
    | beam.Map(lambda employee_count: str(employee_count))
    | beam.io.WriteToText('data/output')
)

p1.run()


