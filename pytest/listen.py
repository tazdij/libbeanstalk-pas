import greenstalk
from pprint import pprint

queue = greenstalk.Client(host='127.0.0.1', port=11300)

queue.watch('sometube')

while True:
    job = queue.reserve()
    pprint(job.id)
    pprint(job.body)
    queue.delete(job)

queue.close()
