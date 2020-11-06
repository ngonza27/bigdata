#Por acción, dia-menor-valor, día-mayor-valor
from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        comp, price, date = line.split(',')
	yield (comp, (price, date))

    def reducer(self, key, values):
	l = list(values)	
	vals = [item[0] for item in l]
	maxd = {}
	maxv = max(vals)
	mind = {}
	minv = min(vals)
	for price, date in l:
		if(maxv == price):
			maxd[price] = date
		if(minv == price):
			mind[price] = date
        yield (key, (("minimo: ", mind), ("maximo: ", maxd)))

if __name__ == '__main__':
    MRWordFrequencyCount.run()



#Listado de acciones que siempre han subido o se mantienen estables.
from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        comp, price, date = line.split(',')
	yield (comp, (date, price))

    def reducer(self, key, values):
	l = list(values)
	l.sort()
	vals = [item[-1] for item in l]
	start = int(vals[0])
	maxv = max(vals)
	acciones = []
	for date, price in l:
		price = int(price)
		start = price
	if (start == int(maxv)):
       		yield "Esta accion subio o se mantuvo:", key
	else:
		yield None, None

if __name__ == '__main__':
    MRWordFrequencyCount.run()
