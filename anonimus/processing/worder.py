import abc
from pathlib import Path
from typing import List, Union

from patronus.processing.mask import IPath

from anonimus.tooling import chunkify, stl


class IWorder(abc.ABC):
    def __init__(self, path: Union[Path, str], do_lower_case: bool = True):
        self.store = set()
        self.do_lower_case = do_lower_case
        with open(str(path), "r") as fin:
            for word in chunkify(fin, sep="\n"):
                self.store.add(word.strip())

    @abc.abstractmethod
    def __call__(self, x, **kwargs):
        pass

    def __iter__(self):
        return stl.NIterator(self.store)


class IStopper(IWorder):
    def __init__(self, path: Union[Path, str] = None, do_lower_case: bool = True, remove_digits: bool = True):
        path = IPath.stopwordspath if path is None else path
        super(IStopper, self).__init__(path, do_lower_case)
        self.remove_digits = remove_digits

    def __call__(self, x, seps: List[str]):
        response = x.strip().lower() if self.do_lower_case else x.strip()
        response = "".join([ch for ch in response if not str(ch).isdigit()])
        for sep in seps:
            words = [w for w in response.split(sep) if w not in self.store]
            response = " ".join(words)
        return response


__all__ = ["IStopper"]
