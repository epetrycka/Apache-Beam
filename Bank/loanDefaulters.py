from typing import List
import apache_beam as beam

def split(record: str) -> List[str]:
    return record.split(',')

with beam.Pipeline() as p:
    input = (
        p
        | 'Read loan' >> beam.io.ReadFromText('./data/loan.txt', skip_header_lines=1)
        | 'Split' >> beam.Map(split)
    )

    loan_def = (
        input
        | 
    )