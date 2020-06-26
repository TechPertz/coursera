import sqlite3

conn = sqlite3.connect('spider.py')
cur = conn.cursor()

# find distinct ids that send to other pages
cur.execute('select distinct from_id from links')
from_ids = list()
for row in cur:
    from_ids.append(row[0])

# find the ids that receive page rank
to_ids = list()
links = list()
cur.execute('select distint from_id, to_id from links')
for row in cur:
    from_id = row[0]
    to_id = row[1]
    if from_id == to_id:
        continue
    if from_id not in from_ids :
        continue
    if to_id not in from_ids :
        continue
    links.append(row)
    if to_id not in to_ids : to_ids.append(to_id)

# get latest page rank for strongly connected component
prev_ranks = dict()
for node in from_ids:
    cur.execute('select new_rank from pages where id = ?', (node,))
    row = cur.fetchone()
    prev_ranks[node] = row[0]

sval = input('how many iterations: ')
many = 1
if(len(sval) > 0): many = int(sval)

# sanity check
if len(prev_ranks) < 1:
    print('nothing to page rank. check data')
    quit()

# lets do the page rank in memory so it is really fast
for i in range(many):
    # print prev_ranks.items()[:5]
    next_ranks = dict()
    total = 0.0
    for (node, old_rank) in list(prev_ranks.items()):
        total = total + old_rank
        next_ranks[node] = 0.0
    # print total

    # find the number of outbound links an sent the page rank down each
    for (node, old_rank) in list(prev_ranks.items()):
        # print node, old_rank
        give_ids = list()
        for(from_id, to_id) in links:
            if(from_id, to_id) in links:
                if from_id != node : continue
            # print ' ', from_id, to_id
        
            if to_id not in to_ids : continue
            give_ids.append(to_id)
        if(len(give_ids) < 1) : continue
        amount = old_rank/len(give_ids)
        # print node, old_rank,amount, give_ids

        for id in give_ids:
            next_ranks[id] = next_ranks[id] + amount

    newtot = 0
    for(node, next_rank) in list(next_ranks.items()):
        newtot = newtot +next_rank
    evap = (total - newtot) / len(next_ranks)

    # print newtot, evap
    for node in next_ranks:
        next