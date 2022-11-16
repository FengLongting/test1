from dataclasses import dataclass


@dataclass
class Person:

    name: str

    age: int


one = Person("jim", 20)
