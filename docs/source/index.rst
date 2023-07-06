:notoc:

.. Raphtory documentation master file, created by

.. module:: Raphtory

****************************************
Raphtory documentation
****************************************

**Date**: |today| **Version**: |version|

**Useful links**:
`Source Repository <https://github.com/Pometry/Raphtory>`__ |
`Issues & Ideas <https://github.com/Pometry/Raphtory/issues>`__ |
`Slack Support <https://join.slack.com/t/raphtory/shared_invite/zt-1b7nhupph-yOG0zlkHspU3tfz2EqcbsA>`__

:mod:`Raphtory` is a platform for building and analysing temporal networks.
The library includes:

* Methods for creating networks from a variety of data sources
* Algorithms to explore their structure and evolution
* Extensible GraphQL server for deployment of applications built on top

Raphtory’s core engine is built in Rust, for efficiency, with Python interfaces, for ease of use.

It is developed by network scientists, with a background in Physics, Applied Mathematics, Engineering and Computer
Science, for use across academia and industry. Read more on our `white paper <https://arxiv.org/abs/2306.16309>`__
and get started below.

.. grid:: 1 2 2 2
    :gutter: 4
    :padding: 2 2 0 0
    :class-container: sd-text-center

    .. grid-item-card::
        :img-top: images/index_getting_started.svg
        :class-card: intro-card
        :shadow: md

        New to *Raphtory*? Learn how to install and follow a tutorial to analyse a graph.

        +++

        .. button-ref:: getting_started
            :ref-type: ref
            :click-parent:
            :color: secondary
            :expand:

            Getting Started


    .. grid-item-card::
        :img-top: images/index_api.svg
        :class-card: intro-card
        :shadow: md

        Learn about the API and the available methods and parameters.

        +++

        .. button-ref:: api
            :ref-type: ref
            :click-parent:
            :color: secondary
            :expand:

            API Reference

    .. grid-item-card::
        :img-top: images/index_contribute.svg
        :class-card: intro-card
        :shadow: md

        Saw a typo? Want to add or improve?

        Learn how to contribute here.

        +++

        .. button-ref:: development
            :ref-type: ref
            :click-parent:
            :color: secondary
            :expand:

            Developer Guide


.. toctree::
    :maxdepth: 3
    :hidden:
    :titlesonly:


    getting_started/index
    development/index
    api/index

