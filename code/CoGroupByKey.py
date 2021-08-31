import apache_beam as beam
from apache_beam.transforms.util import CoGroupByKey


with beam.Pipeline() as p:
    company =(
        p
        |'company'>> beam.Create([
                ('Infosys','1600'),
                ('TCS','3400'),
                ('Wipro','550')
        ])
    )

    ceo = (
        p
        | 'ceo'>>beam.Create(
            [
                ('Infosys','Salil Parekh'),
                ('TCS','Rajesh Gopinathan'),
                ('Wipro','Thierry Delaporte')
            ]
        )
    )

    dict_of_pcoll = ({'company' : company , 'ceo' :ceo})
    
    merge =(
        dict_of_pcoll
        | 'join by key' >> beam.CoGroupByKey()
        | 'print'>> beam.Map(print)
    )

#Output
#('Infosys', {'company': ['1600'], 'ceo': ['Salil Parekh']})
#('TCS', {'company': ['3400'], 'ceo': ['Rajesh Gopinathan']})
#('Wipro', {'company': ['550'], 'ceo': ['Thierry Delaporte']})