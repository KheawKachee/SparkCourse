from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):                #0,Will,33,385
    fields = line.split(',')        #[0,Will,33,385]
    age = int(fields[2])            #33
    numFriends = int(fields[3])     #385
    return (age, numFriends)

lines = sc.textFile("file:///home/kheaw/sparkcourse/fakefriends.csv")                               #/home/kheaw/sparkcourse/fakefriends.csv
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
#(33,(385,1))
#(385+1,1+1) = (386,2) manipulate only key
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
#386/2 = (193,2)
results = averagesByAge.collect()
for result in sorted(results):
    print(result)