import argparse

parser = argparse.ArgumentParser(description='adding numbers together')
parser.add_argument('-n1' ,'--number1', type=int, required=True)
parser.add_argument('-n2' ,'--number2', type=int, required=True)
args = parser.parse_args()

def add(x: int, y: int) -> int:
    result = x+y
    return result

test = add(args.number1, args.number2)

print(test)