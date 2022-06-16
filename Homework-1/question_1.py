from pyspark import SparkContext
import sys


def create_relationships(line):
    tokens = line.split('\t')
    if len(tokens) == 1 or tokens[0] == '':
        return []
    person = int(tokens[0])
    friends = tokens[1].split(',')
    friends_list = [((person, int(friend)), 0) for friend in friends if friend != '']
    mutual_friends_list = []

    # iterating over all combinations of friends because they have a mutual friend
    for index1 in range(0, len(friends) - 1):
        for index2 in range(index1 + 1, len(friends)):
            friend_1 = int(friends[index1])
            friend_2 = int(friends[index2])
            mutual_friends_list.append(((friend_1, friend_2), 1))
            mutual_friends_list.append(((friend_2, friend_1), 1))

    return friends_list + mutual_friends_list


def recommend_people_you_may_know(person_and_non_friends, n=10):
    person, list_of_non_friends = person_and_non_friends
    ordered_list_of_non_friends = sorted(list_of_non_friends,key=lambda pair: (-pair[1],pair[0]))[:n]
    recommendations = map(lambda pair: pair[0], ordered_list_of_non_friends)
    return person, recommendations


if __name__ == "__main__":

    input_filepath = "./data/soc-LiveJournal1Adj.txt"

    sc = SparkContext(appName="hw1-q1-people-you-might-know")
    rdd = sc.textFile(input_filepath)
    relationships = rdd.flatMap(lambda line: create_relationships(line)) # mapper
    already_friends = relationships.filter(lambda pair: pair[1] == 0)
    
    (relationships.
     subtractByKey(already_friends).
     reduceByKey(lambda a, b: a + b). # reducer
     map(lambda pair : (pair[0][0], (pair[0][1], pair[1]))).
     groupByKey().
     mapValues(list).
     map(lambda person_and_non_friends: recommend_people_you_may_know(person_and_non_friends)).
     map(lambda pair: str(pair[0])+"\t"+",".join(map(lambda x: str(x), pair[1]))).
     saveAsTextFile("./output/")
     )