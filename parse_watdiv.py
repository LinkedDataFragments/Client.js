

algorithm = 2
delay = 100
monn = True

results = {}
with open('%seval_algorithm%d.log' % ('monn/' if monn else '', algorithm), 'r') as f:
    for line in f.readlines():
        line = line.strip()
        if not line[0] == '!':
            query = line.strip()
            occs = query.count(' . ')
            if occs not in results:
                results[occs] = [[],[],[],[],[]]
        else:
            vals = line[1:].split(';')
            for idx, val in enumerate(vals):
                if val and val != 'TIMEOUT':
                    results[occs][idx].append(int(val))

with open('%s/avg_algorithm%d_watdiv_%s%dms.csv' % ('monn/' if monn else '', algorithm, 'monn_' if monn else '', delay), 'w') as f:
    f.write('query;time_first;http_first;time;http;results\n');
    for result in results:
        f.write('%d;' % result)
        f.write(';'.join(str(sum(vals)/max(len(vals),1)) for vals in results[result]))
        f.write('\n')
