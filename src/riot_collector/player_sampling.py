"""Bounded deterministic uniform sampling plus least-recently-selected rotation."""
import hashlib
import heapq
import math


class PlayerPool:
    def __init__(self, limit, uniform_fraction, salt):
        self.limit = limit
        self.uniform_slots = min(limit, max(1, math.ceil(limit * uniform_fraction)))
        self.salt = salt
        self.candidates = 0
        self.uniform = []
        self.rotation = []

    @staticmethod
    def _offer(heap, limit, entry):
        if len(heap) < limit:
            heapq.heappush(heap, entry)
        elif entry > heap[0]:
            heapq.heapreplace(heap, entry)

    def add(self, puuid, last_selected, count):
        self.candidates += 1
        last_selected, count = last_selected or 0, count or 0
        def digest(rule):
            return int.from_bytes(hashlib.sha256(f"{self.salt}:{rule}:{puuid}".encode()).digest(), "big")
        self._offer(self.uniform, self.uniform_slots,
                    (-digest("uniform"), puuid, last_selected, count))
        self._offer(self.rotation, self.limit,
                    (-last_selected, -count, -digest("rotation"), puuid, last_selected, count))

    def selected(self):
        result = [(p, "uniform_random", last, count)
                  for _, p, last, count in sorted(self.uniform, reverse=True)]
        chosen = {r[0] for r in result}
        for *_, puuid, last, count in sorted(self.rotation, reverse=True):
            if len(result) >= self.limit:
                break
            if puuid not in chosen:
                result.append((puuid, "least_recently_selected", last, count))
                chosen.add(puuid)
        return result
