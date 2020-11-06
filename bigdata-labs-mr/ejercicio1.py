#El salario promedio por Sector Económico (SE)
from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
	vec = line.split(',')
	se = vec[1]
	salary = int(vec[2])
        yield se, salary

    def reducer(self, key, values):
	lista = list(values)
	prom = sum(lista) / len(lista)
        yield key, prom

if __name__ == '__main__':
    MRWordFrequencyCount.run()
    
    


#El salario promedio por Empleado
from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
	vec = line.split(',')
	idemp = vec[0]
	salary = int(vec[2])
        yield idemp, salary

    def reducer(self, key, values):
	lista = list(values)
	prom = sum(lista) / len(lista)
        yield key, prom

if __name__ == '__main__':
    MRWordFrequencyCount.run()
    
  


#Número de SE por Empleado que ha tenido a lo largo de la estadística
from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
	vec = line.split(',')
	idemp = vec[0]
	se = int(vec[1])
        yield idemp, se

    def reducer(self, key, values):	
	lista = list(values)
        yield key, len(lista)

if __name__ == '__main__':
    MRWordFrequencyCount.run()
