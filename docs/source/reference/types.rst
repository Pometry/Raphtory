typing
============
.. py:module:: raphtory.typing

.. py:type:: PropValue
   :canonical: bool | int | float | datetime | str | Graph | PersistentGraph | Document | list[PropValue] | dict[str, PropValue]

.. py:type:: GID
   :canonical: int | str

.. py:type:: PropInput
   :canonical: Mapping[str, PropValue]

.. py:type:: NodeInput
   :canonical: int | str | Node

.. py:type:: TimeInput
   :canonical: int | str | float | datetime

.. py:type:: Direction
   :canonical: Literal["in", "out", "both"]