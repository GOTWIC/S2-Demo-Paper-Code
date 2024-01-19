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

def convert_numbers_to_ascii(input_string):
    result = ""
    current_numbers = ""

    for char in input_string:
        if char.isdigit():
            current_numbers += char
            if len(current_numbers) == 3:
                ascii_value = int(current_numbers)
                if ascii_value == 999:
                    result += " "
                else:
                    result += chr(ascii_value)
                current_numbers = ""
        else:
            # Non-digit character, append current_numbers if any
            if current_numbers:
                result += chr(int(current_numbers))
                current_numbers = ""
            result += char

    # Append any remaining numbers
    if current_numbers:
        result += chr(int(current_numbers))

    return result

#str1 = "408095|31723|980230|263564|556742|646258|532210|320124|995641|489595|488088|461350|453243|984826|939873|548818|783725|576667|46340|824679|163948|42047|773809|841725|226839|"
#str2 = "-408046|-31673|-980184|-263516|-555743|-645259|-531211|-319125|-994642|-488596|-487089|-460351|-452244|-983827|-938874|-547819|-782726|-575668|-45341|-823680|-162949|-41048|-772810|-840726|-225840|"
#print(add_strings(str1, str2))

input_string = "080105122122097032077097114103104101114105116097999999999999999999999999999"
output_string = convert_numbers_to_ascii(input_string)
print(output_string)
