import numpy as np
from scipy import linalg

M = np.array([[1,2], [2,1], [3,4], [4,3]])
U,Sigma,VT = linalg.svd(M, full_matrices = False)
print("RESULT:\nU: \n",U,"\nSigma:\n",Sigma,"\nV Transpose:\n",VT)

## Eigenvalue decomposition of (M_T)M

MTM = np.dot(np.transpose(M),M)
evals, evecs = linalg.eigh(MTM)
print("Evals and Evecs before rearrangement: \nEvals\n",evals,"\nEvecs: \n",evecs)
sorted_indexes = (-evals).argsort()

# rearrange according to index positions

evals = evals[sorted_indexes]
evecs  = evecs[:,sorted_indexes]

print("-"*40,"\nEvals and Evecs after rearrangement: \nEvals\n",evals,"\nEvecs: \n",evecs)

V = np.transpose(VT)
print("Evecs = V: \nEvecs:\n", evecs, "\nV:\n",V)

# Sigma squared

print("-"*40,"\ndiagonal matrix of MTM is Sigma raised to the power 2:\n",np.power(Sigma,2))