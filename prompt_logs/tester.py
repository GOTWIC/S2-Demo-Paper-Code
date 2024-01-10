# given two strings in the following format: "x|y|z|..."
# split the two strings into lists, add them elementwise, convert each element to a char and then join the list

def add_strings(s1, s2):
    s1 = s1.split("|")
    s2 = s2.split("|")
    s3 = []
    print(s1)
    print(s2)
    for i in range(len(s1)-1):
        s3.append(chr(int(s1[i]) + int(s2[i])))
    return "".join(s3)

str1 = "-414679|-625772|-193755|-272836|-910874|-510511|-615535|-387922|-774641|-417054|-439397|-653722|-298908|-353216|-936566|"
str2 = "414780|625881|193864|272933|910920|510617|615646|388032|774742|417169|439461|653823|299017|353313|936671|"
print(add_strings(str1, str2))
