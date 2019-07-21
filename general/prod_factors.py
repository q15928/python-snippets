"""
Output the list of factors composing of a given number using recursive function 
"""
import copy

def prod_factors(num, result=[]):
    if num == 1:
        print('x'.join([str(_) for _ in result]))
        if 1 not in result:
            result.append(1)
            print('x'.join([str(_) for _ in result]))
    elif num < 0:
        return
    else:
        for i in range(1, num+1):
            if (i == 1 and i not in result) or (i !=1 and num % i == 0):
                newresult = copy.copy(result)
                newresult.append(i)
                prod_factors(num/i, newresult)


prod_factors(8)
""" Output as below:

1x2x2x2
1x2x4
1x4x2
1x8
2x1x2x2
2x1x4
2x2x1x2
2x2x2
2x2x2x1
2x4
2x4x1
4x1x2
4x2
4x2x1
8
8x1
"""