.. _gettingstarted_quickstart:

{{ header }}

=============================================
FAQ - Getting data into Raphtory
=============================================



How do I create a graph?
~~~~~~~~~~~~~~~~~~~~~~~~

.. raw:: html

    <ul class="task-bullet">
        <li>

I want to create a graph

.. ipython:: python

    import raphtory
    g = raphtory.Graph()

To load the raphtory package and start working with it, import the
package. We recommend to import the package under the alias ``raphtory``.


.. raw:: html

        </li>
    </ul>

How do I add nodes and edges?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. raw:: html

    <ul class="task-bullet">
        <li>

I want to add two nodes and an edge into the graph.

.. ipython:: python

    g.add_vertex(0, "Ben")
    g.add_vertex(1, "Hamza")
    g.add_edge(2, "Ben", "Hamza")


Here we have added a node called "Ben" a time 0, and a node called "Hamza" at time 1.
Next we added an edge between "Ben" and "Hamza" at time 2.

.. raw:: html

        </li>
    </ul>

.. note::
    These don't have any properties, but we will add them below!



How do I add nodes and edges with properties?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. raw:: html

    <ul class="task-bullet">
        <li>

I want to add properties with my nodes and edges.

.. ipython:: python

    g.add_vertex(3, "Rachel", {"class": "student", "age": 20})
    g.add_vertex(4, "Shivam", {"class": "student", "age": 21})
    g.add_edge(5, "Rachel", "Shivam", {"class": "friendship"})


Here we have added a node called "Rachel" a time 3, with the properies class and age.
Similarly, we have doen the same for a node called "Shivam" at time 4.
Next we added an edge between "Rachel" and "Shivam" at time 5 with the property name "class" and the value "friendship".

.. raw:: html

        </li>
    </ul>



How do I run an algorithm?
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. raw:: html

    <ul class="task-bullet">
        <li>

I'd like to run a Max Out Degree algorithm.

.. ipython:: python

    from raphtory import algorithms
    print("Graph - Max out degree: %i" %  algorithms.max_out_degree(g))

Here we have imported the algorithms package, and then run the max out degree algorithm on the graph.


.. raw:: html

        </li>
    </ul>



How do I view / visualise my graph?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. raw:: html

    <ul class="task-bullet">
        <li>

You can draw a graph using the `vis` library.
This will render a raphtory graph using either `pyvis` or `matplotlib`.

To use matplotlib, you can do the following:

.. ipython:: python

    from raphtory import vis
    vis.to_networkx(g)


To use pyvis, you can do the following:


.. ipython:: python

    from raphtory import vis
    v = vis.to_pyvis(g)
    v.show('graph.html')

Here we have imported the vis package, and then converted the graph to a networkx / pyvis graph.
We can then view the graph in a notebook, or save it to a file.

.. raw:: html

        </li>
    </ul>

