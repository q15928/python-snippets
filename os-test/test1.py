import random


class SmallerException(Exception):
    pass


def lambda_handler(event, context):
    a = random.randint(1, 30)
    if a > 10:
        print(a)
        return 'SUCCESS'
    else:
        raise SmallerException('FAILURE')
