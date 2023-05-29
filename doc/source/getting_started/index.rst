{{ header }}

.. _getting_started:

===============
Getting started
===============


Installation (python)
------------

.. grid:: 1 2 2 2
    :gutter: 2

    .. grid-item-card:: Install via pip?
        :class-card: install-card
        :columns: 12 12 6 6
        :padding: 3

        Raphtory can be installed via pip from `PyPI <https://pypi.org/project/raphtory>`__.

        ++++

        .. code-block:: bash

            pip install raphtory

    .. grid-item-card:: Prefer rust?
        :class-card: install-card
        :columns: 12 12 6 6
        :padding: 3

        Install into you rust project via crates from `Crates <https://crates.io/crates/raphtory>`__.

        ++++

        .. code-block:: bash

            cargo add raphtory


Building / Source
-----------------

.. grid:: 1 2 2 2
    :gutter: 2

    .. grid-item-card:: Python from source?
        :class-card: install-card
        :columns: 12
        :padding: 3

        Building a specific version? Installing from source? Developing?
        Check the python advanced installation pages.

        +++

        .. button-ref:: install-python
            :ref-type: ref
            :click-parent:
            :color: secondary
            :expand:

            Advanced python installation

    .. grid-item-card:: Rust from source?
        :class-card: install-card
        :columns: 12
        :padding: 3

        Developing a new functionality? You prefer rust over python?

        +++

        .. button-ref:: install-rust
            :ref-type: ref
            :click-parent:
            :color: secondary
            :expand:

            Advanced rust installation


.. _gentle_intro:

Intro to Raphtory
---------------

.. raw:: html

    <div class="container">
    <div id="accordion" class="shadow tutorial-accordion">

        <div class="card tutorial-card">
            <div class="card-header collapsed card-link" data-bs-toggle="collapse" data-bs-target="#collapseOne">
                <div class="d-flex flex-row tutorial-card-header-1">
                    <div class="d-flex flex-row tutorial-card-header-2">
                        <button class="btn btn-dark btn-sm"></button>
                        What kind of data does raphtory handle?
                    </div>

.. raw:: html

                </div>
            </div>
            <div id="collapseOne" class="collapse" data-parent="#accordion">
                <div class="card-body">

Blah Blah Blah Blah.

.. raw:: html

                </div>
            </div>
        </div>

        <div class="card tutorial-card">
            <div class="card-header collapsed card-link" data-bs-toggle="collapse" data-bs-target="#collapseTwo">
                <div class="d-flex flex-row tutorial-card-header-1">
                    <div class="d-flex flex-row tutorial-card-header-2">
                        <button class="btn btn-dark btn-sm"></button>
                        How many algorithms does Raphtory support?
                    </div>

.. raw:: html

                </div>
            </div>
            <div id="collapseTwo" class="collapse" data-parent="#accordion">
                <div class="card-body">

Nlah Nlah Nlah Nlah.

.. raw:: html

                    <div class="d-flex flex-row">
                    </div>
                </div>
            </div>
        </div>

    </div>
    </div>

Tutorials
---------

For a quick overview of Raphtory functionality, see :ref:`intro to raphtory`.


.. If you update this toctree, also update the manual toctree in the
   main index.rst.template

.. toctree::
    :maxdepth: 2
    :hidden:

    overview
    installation/index
    intro_tutorials/index
