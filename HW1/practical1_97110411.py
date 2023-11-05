#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
from scipy.optimize import linprog

n = int(input())
payoff = np.array([list(map(int, input().split())) for i in range(n)])

c = [0] * n + [-1]
A = np.concatenate((np.hstack((-payoff.T, [[1]] * n)), [[1] * n + [0], [-1] * n + [0]]))
b = [0] * n + [1, -1]
x0_bounds = [(0, None) for i in range(n)]
x1_bounds = [(None, None)]
final_bounds = x0_bounds + x1_bounds
res = linprog(c, A_ub = A, b_ub = b, bounds = final_bounds)
player1_probabilities = res.x[:n]

A = np.concatenate((np.hstack((payoff, [[-1]] * n)), [[1] * n + [0], [-1] * n + [0]]))
res = linprog(-np.array(c), A_ub = A, b_ub = b, bounds = final_bounds)
player2_probabilities = res.x[:n]

print(*player1_probabilities, sep=' ')
print(*player2_probabilities, sep=' ')


# In[ ]:




