# Q1.Camel Case
def camel_to_snake(str):
    res=""
    for i in str:
        if(i.isupper()):
            res+="_"+i.lower()
        else:
            res+=i
    return res[1:]
str = "CamelCase"
print(camel_to_snake(str))

'''
Q2:
 
list = [3,0,1,4,6,2,7,10]
 
find the missing element in this list (0,1,2,3.....n)
 
output: [5,8,9]
 '''
def missing_numbers(lst):
    n = max(lst)
    missing_numbers = []
    for i in range(n + 1):
        if i not in lst:
            missing_numbers.append(i)
    return missing_numbers
lst = [3, 0, 1, 4, 6, 2, 7, 10]
print(missing_numbers(lst))
