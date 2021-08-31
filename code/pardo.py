import apache_beam as beam


class SplitWords(beam.DoFn):
    
    def __init__(self, delimeter=':'):
        self.delimeter = delimeter

    def process(self, element):
        for word in element.split(sep=self.delimeter):
            yield word
    

with beam.Pipeline() as p:
    splitwords =(
        p
        |
        'split'>> beam.Create(
            [
                'abc;def;efg;;;;;hij',
                'klm;n:op'
            ]
            |'parallel processing'>>beam.ParDo(SplitWords(';'))
            |'print'>>beam.Map(print)
        )
    )