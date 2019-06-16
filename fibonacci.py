"""
Use python closure to implement Fibonacci
"""

def fib():
    a, b = 0, 1
    print(a, b, end=' ')
    def inner_fib():
        nonlocal a, b
        a, b = b, a+b
        print(b, end=' ')
    return inner_fib

if __name__ == "__main__":
    f= fib()
    for _ in range(15):
        f()
    print()    