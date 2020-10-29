import csv 
import glob
import re
import socket
import multiprocessing
import sys




if len(sys.argv)<4:
    print("""USAGE:
    python3 resolve.py numero_threads csv_objectives column_number""")
    exit()
else:
    hilos = int(sys.argv[1])
    objective = sys.argv[2]
    column_number = int(sys.argv[3]) - 1 
    open("no_resueltos.txt", 'w').close()
    open("resueltos.txt", 'w').close()
def req(i,ips):
    for ip in ips:
        try:
            resolve = socket.gethostbyaddr(ip)
            print(resolve)
            resolveOut = list()
            for ele in resolve:
                if type(ele) == type(list()):
                    if ele:
                        resolveOut.append(ele[0])
                else:
                    resolveOut.append(ele)
            with open("resueltos.txt", 'a') as f:
                f.write(f"{resolveOut[1]},{resolveOut[0]}\n")
        except:
            print(ip,"no resuelta")
            with open("no_resueltos.txt", 'a') as f:
                f.write(f'{ip}\n')
def chunks(l, n):
    return [l[i:i + n] for i in range(0, len(l), n)]


def dispatch_jobs(data, job_number):
    total = len(data)
    chunk_size = total // job_number
    slice = chunks(data, chunk_size)
    jobs = []

    for i, s in enumerate(slice):
        j = multiprocessing.Process(target=req, args=(i, s))
        jobs.append(j)
    for j in jobs:
        j.start()


if __name__ ==  '__main__':

    csvs = glob.glob(objective)
    patIp = re.compile("^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
    conj = set()
    for csv_file in csvs:
        with open(csv_file, 'r',encoding="utf8") as f:
            csv_reader = csv.reader(f, delimiter=',')
            
            line_count = 0
            print(f"Nombre del CSV: {csv_file}")
            
            for row in csv_reader:
                host = row[column_number]
                if patIp.match(host):
                    conj.add(host)
                line_count += 1
            print(f"lineas:{line_count} total de ips no resueltas:{len(conj)}")

    dispatch_jobs(list(conj), hilos)
            



