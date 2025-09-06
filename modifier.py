from typing import Self, Callable, Any

import pandas as pd
from faker import Faker

from exceptions import APOLOGIES

default_faker = Faker()


class Modifier:
    def __init__(self, frame: pd.DataFrame, inplace_modification: bool = True, faker: Faker = default_faker) -> None:
        if not inplace_modification:
            raise NotImplementedError(f"Must be inplace actually, {APOLOGIES}")
        self._frame = frame
        self._faker = faker

    def result(self) -> pd.DataFrame:
        return self._frame

    # Don't mind that, it's just some simplification
    def _apply_for_each_row_in_column(self, column: str, modification_function: Callable[[Any], Any]):
        self._frame[column] = self._frame[column].apply(modification_function)
    def _apply_for_each_row_in_column_and_return_modifier(self, column: str, modification_function: Callable[[Any], Any]) -> Self:
        self._apply_for_each_row_in_column(column, modification_function)
        return self

    # Randomizers
    # TODO: try to export the faker in the apply_function
    def apply_function(self, column: str, modification_function: Callable[[], Any]) -> Self:
        return self._apply_for_each_row_in_column_and_return_modifier(column, lambda row: modification_function())
    def randomize_last_name(self, column: str) -> Self:
        return self.apply_function(column, self._faker.last_name)
    def randomize_first_name(self, column: str) -> Self:
        return self.apply_function(column, self._faker.first_name)
    def randomize_address(self, column: str) -> Self:
        return self.apply_function(column, self._faker.address)
