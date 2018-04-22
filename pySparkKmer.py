#Step 1: imports
import findspark
import argparse
import os
from operator import add
import shutil
import re
findspark.init(os.environ['SPARK_HOME'])
import pyspark
from pyspark.context import SparkContext



def main():
    #Step 2: handle input parameters
    parser = argparse.ArgumentParser(description='Process some integers.')
    #filenames, K and N
    parser.add_argument('-filenames', type=str, nargs='+',
                    help='the list of fasta files', required=True)
    parser.add_argument('-K', type=int, nargs=1, help='value of K in k-mer', required=True)
    parser.add_argument('-N', type=int, nargs=1, help='value of N in top-n', required=True)
    args = parser.parse_args()

    #Step 3: create a Spark context object (ctx)
    ctx = SparkContext(appName="Kmer count")

    #Step 4: broadcast K and N as global shared objects
    files = ctx.broadcast(args.filenames)
    k = ctx.broadcast(args.K)
    n = ctx.broadcast(args.N)
    print(files.value)
    print(k.value)
    print(n.value)


    #Step 5: read FASTQ file from HDFS and create the first RDD
    records = ctx.textFile(files.value[0])
    #remove file if exists
    # try:
    #     shutil.rmtree("kmers/output/1")
    #     print("removed old output")
    # except OSError:
    #     print("kmers / output / 1 did not exist, creating now")
    # records.saveAsTextFile("kmers/output/1")

    #Step 6: filter redundant records
    #specChar = re.compile('[A-Za-z]')
    pattern = re.compile('^[ACGTNacgn]+$')
    records = records.filter(lambda x: re.match(pattern, x) != None)#.filter(lambda x: re.match(specChar,x)!=None)

    # for i in filterRDD.collect():
    #     print(i)
    # try:
    #     shutil.rmtree("kmers/output/1.5")
    #     print("removed old output")
    # except OSError:
    #     print("kmers / output / 1.5 did not exist, creating now")
    # filterRDD.saveAsTextFile("kmers/output/1.5")

    # Step 7: generate K-mers
    kVal = k.value[0]
    kmers = records.map(
            lambda x: (x[0:kVal],1))
    # for k in kmers.collect():
    #     print(k)
    # try:
    #     shutil.rmtree("kmers/output/2")
    #     print("removed old output")
    # except OSError:
    #     print("kmers / output / 2 did not exist, creating now")
    # kmers.saveAsTextFile("kmers/output/2")

    # Step 8: Combine/reduce frequent K-mers
    grouped = kmers.reduceByKey(lambda x,y: x+y)
    # try:
    #     shutil.rmtree("kmers/output/2.5")
    #     print("removed old output")
    # except OSError:
    #     print("kmers / output / 2.5 did not exist, creating now")
    # grouped.saveAsTextFile("kmers/output/2.5")

    # Step 9: create a local top N for all partitions
    sortedKmers = grouped.map(lambda x: (int(-x[1]),x[0])).sortByKey().map(lambda x: (x[1], -1*int(x[0])))
    try:
        shutil.rmtree("kmers/output/3")
        print("removed old output")
    except OSError:
        print("kmers / output / 3 did not exist, creating now")
    sortedKmers.saveAsTextFile("kmers/output/3")
    
    #Step 10: get top N
    print("Top N={} {}-mers:".format(n.value[0],k.value[0]))
    for val in sortedKmers.take(n.value[0]):
        print(val)
    # print("Bottom N={} {}-mers:".format(n.value[0], k.value[0]))
    # for val in sortedKmers.takeOrdered(n.value[0], key=lambda x:-1*int(x[1])):
    #     print(val)

if __name__ == '__main__':
    main()
    
