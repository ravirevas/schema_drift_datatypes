from collections import defaultdict
class DictUtil(object):
    """
    Calculate the difference between two dictionaries as:
    (1) items added
    (2) items removed
    (3) keys same in both but changed values
    (4) keys same in both and unchanged values
    """
    def __init__(self, current_dict, past_dict):
        self.current_dict, self.past_dict = current_dict, past_dict
        self.set_current, self.set_past = set(current_dict.keys()), set(past_dict.keys())
        self.intersect = self.set_current.intersection(self.set_past)

    def added(self):
        return self.set_current - self.intersect
    def removed(self):
        return self.set_past - self.intersect
    def changed(self):
        return list((o,(self.past_dict[o],self.current_dict[o])) for o in self.intersect if self.past_dict[o] != self.current_dict[o])
    def unchanged(self):
        return set(o for o in self.intersect if self.past_dict[o] == self.current_dict[o])


def main():
    a = {'a': '1', 'b': '1', 'c': '0'}
    b = {'a': '1', 'b': '2', 'c':'1'}
    df =  DictUtil(a,b)
    print(df.added())
    print('#########')
    print(df.changed())

if __name__== "__main__":
    print(main())

