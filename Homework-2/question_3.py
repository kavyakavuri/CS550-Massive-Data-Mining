import numpy as np

'''
Page rank algorithm

r_new = ((1 -beta)/n)*o + beta*M*r_prev

r = PageRank column vector (n x 1)
beta = scalar (1 - beta) -> teleportation probability
n = no. of nodes (scalar)
o= (n x 1) vector containing all 1s
M = Matrix (n x n) defined by the formula below:

       | 1/deg(i)  if edge exists from i -> j
M_ji = |
       | 0         else

deg(i) is number of outgoing edges of node i

Algorithm:

1. Initialize r_0 = (1/n)*o
2. Iterate k times (i from 1 to k) : r_i = ((1 -beta)/n)*o + beta*M*r_(i - 1)

'''



n = 100    # number of nodes in graph
beta = 0.8 # beta
iterations = 40
M = np.zeros((n,n)) # Matrix M
r = np.zeros((n,1)) # Page rank vector r
one = np.ones((n,1)) # vector o
input_file = 'data/graph.txt'


with open(input_file) as fp:
   line = fp.readline()
   while line:
       tokens = line.strip().split()
       i = tokens[0]  # source  
       j = tokens[1]  # target
       M[int(j)-1][int(i)-1] += 1 # calculating number of edges for all i -> j
       line = fp.readline()

# compute out-degree of nodes
deg_col = np.sum(M, axis = 0)

# divide each cell in column i with deg(i) to get final M
for i in range(n):
    M[:,i] = np.true_divide(M[:,i],deg_col[i])

# computing rank
r = one/n # initialization (See step 1.)
teleport_term = one * ((1-beta)/n)

for i in range(iterations):
    r =  teleport_term + np.matmul(beta*M,r)

# sorting nodes by page rank
temp = np.transpose(r)
idx = temp.argsort()
max_nodes = idx[0][-5:][::-1]+1
min_nodes = idx[0][:5] + 1

print("Top 5 nodes with highest page rank score: ", max_nodes)
print("Node ID \t Page rank")
for i in max_nodes:
    print(i, "\t\t", r[i-1][0])

print("Bottom 5 nodes with lowest page rank: ", min_nodes)
print("\nNode ID \t Page rank")
for i in min_nodes:
    print(i, "\t\t", r[i-1][0])