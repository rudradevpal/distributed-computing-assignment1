"""
vector_clock.py
---------------
Vector clock for N processes.

The three rules (Mattern / Fidge):

  R1  internal event at Pi        : V[i] += 1
  R2  send at Pi                  : V[i] += 1, then piggyback a copy of V
  R3  receive at Pi of message m  : V[i] += 1, then V[j] = max(V[j], m.V[j]) for all j

Comparison of two vector timestamps a and b:

  a -> b  ("a happened before b")   iff  a[k] <= b[k] for all k, and a != b
  a || b  ("a concurrent with b")   iff  neither a -> b nor b -> a
"""

BEFORE = "BEFORE"
AFTER = "AFTER"
EQUAL = "EQUAL"
CONCURRENT = "CONCURRENT"


class VectorClock:
    def __init__(self, n, idx):
        self.n = n
        self.idx = idx
        self.v = [0] * n

    def tick(self):
        """Rule R1/R2/R3 first half: advance this process's own slot."""
        self.v[self.idx] += 1
        return list(self.v)

    def merge(self, other):
        """Rule R3 second half: element-wise maximum with a received vector."""
        for i in range(self.n):
            if other[i] > self.v[i]:
                self.v[i] = other[i]

    def copy(self):
        return list(self.v)

    def __str__(self):
        return "(" + ",".join(str(x) for x in self.v) + ")"


def fmt(v):
    """Render a vector timestamp the way it appears in logs: (1,0,0,0)"""
    return "(" + ",".join(str(x) for x in v) + ")"


def compare(a, b):
    """Return BEFORE, AFTER, EQUAL or CONCURRENT for two vector timestamps."""
    less_or_eq = all(x <= y for x, y in zip(a, b))
    greater_or_eq = all(x >= y for x, y in zip(a, b))

    if less_or_eq and greater_or_eq:
        return EQUAL
    if less_or_eq:
        return BEFORE
    if greater_or_eq:
        return AFTER
    return CONCURRENT


def happened_before(a, b):
    return compare(a, b) == BEFORE


def concurrent(a, b):
    return compare(a, b) == CONCURRENT
