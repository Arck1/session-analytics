def repair(line):
    if len(line) > 2 and line[-1] == "\n" and line[-2] != ',':
        line = line[:-1] + "},\n"
    return line


def repair_file(file_name):
    buffer = []
    with open(file_name, 'r') as file:
        for line in file:
            if 'fvisit' in line:
                line = repair(line)
            buffer.append(line)

    with open("out" + file_name, 'w') as file:
        file.writelines(buffer)


if __name__ == '__main__':
    repair_file("test.json")

