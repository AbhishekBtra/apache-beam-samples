import apache_beam as beam

mcaps = ['large','mid','small']

def partition_by_mcap(item, num_of_partitions):
    return mcaps.index(item['mcap_type'])

with beam.Pipeline() as p:
    large_stocks, mid_stocks, small_stocks =(  ## this order should be same as mcaps list order
        p
        |
        'create'>> beam.Create(
            [
                {
                    'name':'sbi',
                    'mcap_type':'large',
                    'price':450
                },
                {
                    'name':'hdfc',
                    'mcap_type':'large',
                    'price':1550
                },
                {
                    'name':'icici',
                    'mcap_type':'large',
                    'price':550
                },
                {
                    'name':'havells',
                    'mcap_type':'mid',
                    'price':1250
                },
                {
                    'name':'irb infra',
                    'mcap_type':'small',
                    'price':150
                },
            ]
        )
        |'create partitions'>> beam.Partition(partition_by_mcap,len(mcaps))
    )

    large_stocks | 'large stocks' >> beam.Map(lambda x : print('large => {} '.format(x)))
    mid_stocks | 'mid stocks' >> beam.Map(lambda x : print('mid => {}'.format(x)))
    small_stocks | 'small stocks' >> beam.Map(lambda x : print('small => {}'.format(x)))

#Output
#large => {'name': 'sbii', 'mcap_type': 'large', 'price': 450}
#large => {'name': 'hdfc', 'mcap_type': 'large', 'price': 1550}
#large => {'name': 'icici', 'mcap_type': 'large', 'price': 550}
#mid => {'name': 'havells', 'mcap_type': 'mid', 'price': 1250}
#small => {'name': 'irb infra', 'mcap_type': 'small', 'price': 150}