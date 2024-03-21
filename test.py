f = open('vals.txt', 'rb')

data = f.read(1024)

print(data)
x = []
for val in data:
    print(val)
    if val == 10:
        break
    else:
        x.append(val)

print(x)