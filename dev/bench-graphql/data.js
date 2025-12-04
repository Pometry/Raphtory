window.BENCHMARK_DATA = {
  "lastUpdate": 1764883037094,
  "repoUrl": "https://github.com/Pometry/Raphtory",
  "entries": {
    "GraphQL Benchmark": [
      {
        "commit": {
          "author": {
            "email": "james.baross@pometry.com",
            "name": "James Baross",
            "username": "jbaross-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ceafe286febccb4e5a7c763ae6b5c026b4ec2726",
          "message": "James/graphql docstrings (#2210)\n\n* init\n\n* docstrings\n\n* docstrings for edges\n\n* docstrings for edges\n\n* regen schema and docs\n\n* run formatting\n\n* chore: apply tidy-public auto-fixes\n\n* more docstrings\n\n* backticks are not used by docs parser so remove\n\n* more docstrings\n\n* chore: apply tidy-public auto-fixes\n\n* more docstrings\n\n* chore: apply tidy-public auto-fixes\n\n* more docstrings\n\n* more docstrings\n\n* update schema and format\n\n* chore: apply tidy-public auto-fixes\n\n* more docstrings\n\n* chore: apply tidy-public auto-fixes\n\n* more docstrings\n\n* chore: apply tidy-public auto-fixes\n\n* more docstrings\n\n* chore: apply tidy-public auto-fixes\n\n* more docstrings\n\n* more docstrings\n\n* cleanup\n\n* more docstrings\n\n* chore: apply tidy-public auto-fixes\n\n* testcase for inputs\n\n* cleanup\n\n* chore: apply tidy-public auto-fixes\n\n* more docstrings\n\n* cleanup\n\n* chore: apply tidy-public auto-fixes\n\n* more docstrings\n\n* cleanup\n\n* chore: apply tidy-public auto-fixes\n\n* fix page  docstrings\n\n* cleanup\n\n* remove latin\n\n* chore: apply tidy-public auto-fixes\n\n* initial fixes\n\n* fmt\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>\nCo-authored-by: Ben Steer <ben.steer@pometry.com>",
          "timestamp": "2025-08-14T13:30:32+01:00",
          "tree_id": "e5bb1e8da7e6748e41304ab0f3bc68b18c91e8b8",
          "url": "https://github.com/Pometry/Raphtory/commit/ceafe286febccb4e5a7c763ae6b5c026b4ec2726"
        },
        "date": 1755176737038,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": null,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 294,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 209,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1206,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 1070,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1629,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "41898282+github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "b81c0c12effe8ed93a5398c6115f010589f8f1c7",
          "message": "Release v0.16.1 (#2236)\n\nchore: Release\n\nCo-authored-by: Pometry-Team <ben.steer@pometry.com>",
          "timestamp": "2025-08-14T15:19:03+01:00",
          "tree_id": "ffb78d200789a7b11784a754d138e02ce9fdcd25",
          "url": "https://github.com/Pometry/Raphtory/commit/b81c0c12effe8ed93a5398c6115f010589f8f1c7"
        },
        "date": 1755183236000,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 19,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 266,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 209,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1342,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 1100,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1592,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "3e12628a3205b1a6ee7ca755c840c8cf8a837941",
          "message": "Fix explode layers for filtered persistent graph (#2241)\n\n* explode layers for valid graph is broken\n\n* explode_layers was ignoring layer filters for persistent semantics\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-08-20T16:18:14+02:00",
          "tree_id": "53e3e4967888a8a9f86eb4de5ac36693a67f692a",
          "url": "https://github.com/Pometry/Raphtory/commit/3e12628a3205b1a6ee7ca755c840c8cf8a837941"
        },
        "date": 1755701595767,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": null,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 277,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 236,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1394,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 1074,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1493,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "james.baross@pometry.com",
            "name": "James Baross",
            "username": "jbaross-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "0a6040906efb77b157b9810986ebe1bf20dc5f69",
          "message": "James/graphql docstrings fixes (#2239)\n\n* init\n\n* docstrings\n\n* docstrings for edges\n\n* docstrings for edges\n\n* regen schema and docs\n\n* run formatting\n\n* chore: apply tidy-public auto-fixes\n\n* more docstrings\n\n* backticks are not used by docs parser so remove\n\n* more docstrings\n\n* chore: apply tidy-public auto-fixes\n\n* more docstrings\n\n* chore: apply tidy-public auto-fixes\n\n* more docstrings\n\n* more docstrings\n\n* update schema and format\n\n* chore: apply tidy-public auto-fixes\n\n* more docstrings\n\n* chore: apply tidy-public auto-fixes\n\n* more docstrings\n\n* chore: apply tidy-public auto-fixes\n\n* more docstrings\n\n* chore: apply tidy-public auto-fixes\n\n* more docstrings\n\n* more docstrings\n\n* cleanup\n\n* more docstrings\n\n* chore: apply tidy-public auto-fixes\n\n* testcase for inputs\n\n* cleanup\n\n* chore: apply tidy-public auto-fixes\n\n* more docstrings\n\n* cleanup\n\n* chore: apply tidy-public auto-fixes\n\n* more docstrings\n\n* cleanup\n\n* chore: apply tidy-public auto-fixes\n\n* fix page  docstrings\n\n* cleanup\n\n* remove latin\n\n* chore: apply tidy-public auto-fixes\n\n* initial fixes\n\n* fmt\n\n* chore: apply tidy-public auto-fixes\n\n* fix double spacing\n\n* review fixes\n\n* specify layers for has_edge\n\n* tidy\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>\nCo-authored-by: Ben Steer <ben.steer@pometry.com>\nCo-authored-by: Ben Steer <b.a.steer@qmul.ac.uk>",
          "timestamp": "2025-08-22T15:10:48+01:00",
          "tree_id": "2815be5be17ac0210ded7825a45aaae1beaa7d05",
          "url": "https://github.com/Pometry/Raphtory/commit/0a6040906efb77b157b9810986ebe1bf20dc5f69"
        },
        "date": 1755873945915,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1470,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 287,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 211,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1311,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 1086,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1529,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "james.baross@pometry.com",
            "name": "James Baross",
            "username": "jbaross-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "d085e740177fc8fbb9c97a8d3b37a6e8862b5419",
          "message": "James/graphql-userguide-16-x (#2233)\n\n* update ui  image\n\n* mutation and views\n\n* persistent and event distinction\n\n* clean up running steps and add cli\n\n* subtitle\n\n* proper hierarchy\n\n* props and metadata examples\n\n* Add troubleshooting\n\n* add missing cli parameter\n\n* chore: apply tidy-public auto-fixes\n\n* add default save location to troubleshooting\n\n* markdown formatting\n\n* chore: apply tidy-public auto-fixes\n\n* chore: apply tidy-public auto-fixes\n\n* Clarify docker basics\n\n* Clarify docker basics\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>\nCo-authored-by: Ben Steer <b.a.steer@qmul.ac.uk>",
          "timestamp": "2025-08-24T17:59:14+01:00",
          "tree_id": "3cf4543e7612dc4590d316041fe0ef7d6c9b7266",
          "url": "https://github.com/Pometry/Raphtory/commit/d085e740177fc8fbb9c97a8d3b37a6e8862b5419"
        },
        "date": 1756056843293,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 21,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 273,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 248,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1325,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 1061,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 890,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "5086c5ae9aef1bba0407148822917dac319bdc03",
          "message": "add docker retag action (#2245)\n\n* add docker retag action\n\n* add permissions\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-08-26T15:52:15+01:00",
          "tree_id": "cac9e34994f0b0b265b1744605f03124c7cb4418",
          "url": "https://github.com/Pometry/Raphtory/commit/5086c5ae9aef1bba0407148822917dac319bdc03"
        },
        "date": 1756222039749,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1502,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 278,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 204,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1274,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 1060,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1660,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ed.sherrington@pometry.com",
            "name": "edsherrington",
            "username": "edsherrington"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f14de7dcd7933e0c851989b04cf180ca96faf3b7",
          "message": "update Slack invite link (#2252)",
          "timestamp": "2025-09-08T23:13:08+01:00",
          "tree_id": "cc9c7060ba34cfea04cb107d6499379b9d075d3e",
          "url": "https://github.com/Pometry/Raphtory/commit/f14de7dcd7933e0c851989b04cf180ca96faf3b7"
        },
        "date": 1757371716743,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1460,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 279,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 215,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1273,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 1020,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1538,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "432df6c48990736945c323fd0b683e31045fed03",
          "message": "Increase sleep time on graphql bench (#2278)\n\nUpdate Makefile",
          "timestamp": "2025-09-15T16:32:17+01:00",
          "tree_id": "90b035aebd0ed3574b1d35e09c41253a363b401f",
          "url": "https://github.com/Pometry/Raphtory/commit/432df6c48990736945c323fd0b683e31045fed03"
        },
        "date": 1757952426619,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1458,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 276,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 202,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1207,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 1047,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1586,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "49699333+dependabot[bot]@users.noreply.github.com",
            "name": "dependabot[bot]",
            "username": "dependabot[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "4eb2b5ae8247ed9e6d8ab875ce9fab04a4f9f4be",
          "message": "Bump tracing-subscriber from 0.3.19 to 0.3.20 in the cargo group across 1 directory (#2251)\n\n* Bump tracing-subscriber in the cargo group across 1 directory\n\nBumps the cargo group with 1 update in the / directory: [tracing-subscriber](https://github.com/tokio-rs/tracing).\n\n\nUpdates `tracing-subscriber` from 0.3.19 to 0.3.20\n- [Release notes](https://github.com/tokio-rs/tracing/releases)\n- [Commits](https://github.com/tokio-rs/tracing/compare/tracing-subscriber-0.3.19...tracing-subscriber-0.3.20)\n\n---\nupdated-dependencies:\n- dependency-name: tracing-subscriber\n  dependency-version: 0.3.20\n  dependency-type: direct:production\n  dependency-group: cargo\n...\n\nSigned-off-by: dependabot[bot] <support@github.com>\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>\nCo-authored-by: Ben Steer <b.a.steer@qmul.ac.uk>",
          "timestamp": "2025-09-16T13:39:39+01:00",
          "tree_id": "3b75c605bf3d09356afbbf5dc9a219a6a368b67f",
          "url": "https://github.com/Pometry/Raphtory/commit/4eb2b5ae8247ed9e6d8ab875ce9fab04a4f9f4be"
        },
        "date": 1758028477778,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1466,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 280,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 214,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1319,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 1067,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1667,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "james.baross@pometry.com",
            "name": "James Baross",
            "username": "jbaross-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e3c83992f17a7f3f4124913fc77d7e3d3f15b6bf",
          "message": "Use raphtory from python dir (#2275)\n\n* Use raphtory from python dir\n\n* comment to match\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-09-16T15:59:06+01:00",
          "tree_id": "f7088a8b3ad1b977d16ad1b0cabcb09433e02fc4",
          "url": "https://github.com/Pometry/Raphtory/commit/e3c83992f17a7f3f4124913fc77d7e3d3f15b6bf"
        },
        "date": 1758036873573,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1452,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 290,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 218,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1288,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 1065,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1558,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "2404621+fabianmurariu@users.noreply.github.com",
            "name": "Fabian Murariu",
            "username": "fabianmurariu"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "0c6115747197d0e9bfc4312dd09e5bbe3610a8ec",
          "message": "Add EIDS to Node addition (#2279)\n\n* added support for edge_history in LayerAdditions\n\n* eids added to node additions\n\n* add more simple tests\n\n* push the edge_history into the higher level APIs\n\n* fix broken test\n\n* added edge_history to NodeView\n\n* added tests for NodeView\n\n* chore: apply tidy-public auto-fixes\n\n* kmerge the slow edge history for nodes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-09-17T15:58:47+01:00",
          "tree_id": "94d0007116b75bcc5fc591345203b6aee5a2418d",
          "url": "https://github.com/Pometry/Raphtory/commit/0c6115747197d0e9bfc4312dd09e5bbe3610a8ec"
        },
        "date": 1758123216603,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1471,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 294,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 226,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1300,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 1010,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1727,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "db0c9716d5e1d84efccde7a4201da75c86f6769a",
          "message": "Removed last graphql objects with gql in the name (#2283)\n\n* Removed last graphql objects with gql in the name\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-09-19T18:12:25+01:00",
          "tree_id": "a08c3a3dc639b1d4cf6b24adc55fb2ab5ece9869",
          "url": "https://github.com/Pometry/Raphtory/commit/db0c9716d5e1d84efccde7a4201da75c86f6769a"
        },
        "date": 1758304048164,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1404,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 291,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 225,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1248,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 1039,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1663,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "2404621+fabianmurariu@users.noreply.github.com",
            "name": "Fabian Murariu",
            "username": "fabianmurariu"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ccacea18d3ea750e7149fe75a5739ecb5704803f",
          "message": "Indexed node additions and moves tests into separate raphtory/tests (#2289)\n\n* move raphtory tests into raphtory/tests/\n\n* break away proto and df loaders tests\n\n* import Materialized graph\n\n* bring debug symbols back and remove some of the warnings from the new tests\n\n* missing use\n\n* fixed comments on PR\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>\nCo-authored-by: Ben Steer <b.a.steer@qmul.ac.uk>",
          "timestamp": "2025-09-30T12:30:37+01:00",
          "tree_id": "aa8722b25b36ff4de415b5d7875ed00e6516aab5",
          "url": "https://github.com/Pometry/Raphtory/commit/ccacea18d3ea750e7149fe75a5739ecb5704803f"
        },
        "date": 1759234032456,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1443,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 269,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 211,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1168,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 1043,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1627,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "james.baross@pometry.com",
            "name": "James Baross",
            "username": "jbaross-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "0f0f9649651f7745648177dc92a969196e9efa18",
          "message": "James/2250 temporal vs plain filtering (#2286)\n\n* What are windows\n\n* clarify time filters vs filter module\n\n* add filtering topic\n\n* better intro\n\n* motivate filtering\n\n* motivate filtering\n\n* add filtering example\n\n* add filtering example\n\n* improve filtering description\n\n* typo\n\n* review fixes\n\n* review fixes\n\n* autogen\n\n* fix csv file path\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-10-01T09:54:44+01:00",
          "tree_id": "02dbaa35dc22778746e5853f66ebcbd57202bee3",
          "url": "https://github.com/Pometry/Raphtory/commit/0f0f9649651f7745648177dc92a969196e9efa18"
        },
        "date": 1759311094813,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1329,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 242,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 168,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1181,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 956,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1233,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "fbd7326250de52326693dca6cd54ce6763c74bff",
          "message": "bump rust version for release action (#2298)\n\n* bump rust version for release action\n\n* chore: apply tidy-public auto-fixes\n\n* change rust version to stable for the bump versions action\n\n* setup stable rust on _release_rust.yml as well\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-10-01T17:13:59+02:00",
          "tree_id": "c4102f05e4f220d949e6e8cb150fbc167c58bc99",
          "url": "https://github.com/Pometry/Raphtory/commit/fbd7326250de52326693dca6cd54ce6763c74bff"
        },
        "date": 1759333820910,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1427,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 274,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 222,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1144,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 1018,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1519,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ffea1ff8359d9f2829018f6a4cb5cb91b4caad28",
          "message": "add action for docker build cloud (#2309)\n\n* add action for docker build cloud\n\n* setup multi-platform\n\n* reset pometry storage version",
          "timestamp": "2025-10-02T18:51:28+02:00",
          "tree_id": "a5c795ce914698366b17c9675fb6abe8cdc516a0",
          "url": "https://github.com/Pometry/Raphtory/commit/ffea1ff8359d9f2829018f6a4cb5cb91b4caad28"
        },
        "date": 1759426046466,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1426,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 255,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 188,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1274,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 1022,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1593,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "19954b32ab53d392806793daff914ed81011d5dd",
          "message": "point to the correct docker path (#2310)\n\n* point to the correct docker path\n\n* turn into manual action\n\n* chore: apply tidy-public auto-fixes\n\n* hadrcode builder name\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-10-02T19:59:31+02:00",
          "tree_id": "60cce4660ffab8f17cb0e26e5966a8a0981ef4cf",
          "url": "https://github.com/Pometry/Raphtory/commit/19954b32ab53d392806793daff914ed81011d5dd"
        },
        "date": 1759430157513,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1405,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 274,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 178,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1228,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 997,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1306,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "a787ddd10fe7bbf9d20295f33300779943c317b2",
          "message": "fix action to build in docker build cloud (#2315)\n\n* try removing .git\n\n* use context .",
          "timestamp": "2025-10-03T16:56:58+01:00",
          "tree_id": "02175d75102f2ab24c7936774d955c3ee8c236bd",
          "url": "https://github.com/Pometry/Raphtory/commit/a787ddd10fe7bbf9d20295f33300779943c317b2"
        },
        "date": 1759509203830,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1417,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 259,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 211,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1238,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 999,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1424,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "b9acb71665ed9c4c40b188b93d3017c0c9cf4f6c",
          "message": "optimise simple temporal intervals (#2320)",
          "timestamp": "2025-10-09T11:58:02+02:00",
          "tree_id": "76882d6ed43f3c418ca8988b056e54b935aab791",
          "url": "https://github.com/Pometry/Raphtory/commit/b9acb71665ed9c4c40b188b93d3017c0c9cf4f6c"
        },
        "date": 1760006065802,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1371,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 253,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 188,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1219,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 912,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1524,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "23f65c8be945341596af1446f8c407986feaeea6",
          "message": "timeline start/end should use global earliest and latest time (#2319)\n\n* add tests for layers and rolling\n\n* timeline start/end should use global earliest and latest time in fallback, not filtered time as rolling/expanding for different layers etc. should align by default\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-10-09T15:03:53+02:00",
          "tree_id": "05a1ff6b2c6dd5b67d03cc10e26b2e61d6177962",
          "url": "https://github.com/Pometry/Raphtory/commit/23f65c8be945341596af1446f8c407986feaeea6"
        },
        "date": 1760017214881,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 0,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 256,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 214,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1233,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 1007,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1575,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "james.baross@pometry.com",
            "name": "James Baross",
            "username": "jbaross-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c120365f34769ce1a753d819c7fa96e4f5af35ad",
          "message": "remove extra newline in macro docstrings (#2323)\n\n* remove extra newline in macro docstrings\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-10-09T15:04:01+01:00",
          "tree_id": "f40e2cb6d5779f5a91671f13d8227da3df0e0f65",
          "url": "https://github.com/Pometry/Raphtory/commit/c120365f34769ce1a753d819c7fa96e4f5af35ad"
        },
        "date": 1760020824221,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 19,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 277,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 211,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1006,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 966,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1508,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "de3b0b56d96721bdae93eb879a8aa4fc66444b7d",
          "message": "Deadlock fixes and concurrency configuration from 0.16 (#2324)\n\n* Fix deadlock (#2292)\n\n* make a write pool\n\n* use blocking_write for all graph mutations\n\n* enable tests\n\n* chore: apply tidy-public auto-fixes\n\n* fix the recursive read deadlock\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>\n\n* Release v0.16.2 (#2299)\n\nchore: Release\n\nCo-authored-by: Pometry-Team <ben.steer@pometry.com>\n\n* Deadlock fixes and concurrency configuration (#2313)\n\n* make all the rw_lock.read be read_recursive\n\n* fixes deadlocks\n\n* fixes Python deadlock\n\n* add a sempahore in front of the graphql query execution\n\n* sort pr and add env variable to be able to set concurrency limit\n\n* add rwlock in the middleware\n\n* loop when taking the PairEntryMut lock\n\n* make rwlock optional\n\n* chore: apply tidy-public auto-fixes\n\n* rename env var for concurrency limit\n\n---------\n\nCo-authored-by: Fabian Murariu <murariu.fabian@gmail.com>\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: ljeub-pometry <97447091+ljeub-pometry@users.noreply.github.com>\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>\nCo-authored-by: github-actions[bot] <41898282+github-actions[bot]@users.noreply.github.com>\nCo-authored-by: Pometry-Team <ben.steer@pometry.com>\nCo-authored-by: Pedro Rico Pinazo <ricopinazo@gmail.com>\nCo-authored-by: Fabian Murariu <murariu.fabian@gmail.com>",
          "timestamp": "2025-10-10T09:48:13+01:00",
          "tree_id": "f325d4c937ba250924193ba71c2826985644e20c",
          "url": "https://github.com/Pometry/Raphtory/commit/de3b0b56d96721bdae93eb879a8aa4fc66444b7d"
        },
        "date": 1760088273400,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1512,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 225,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 149,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1190,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 1017,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1578,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c62aa568b2acfdcbd9a87b23dc2179eb4980d417",
          "message": "not all nodes are guaranteed to be initialised in the iterators (#2325)\n\n* not all nodes are guaranteed to be initialised in the iterators\n\n* missing cfg\n\n* fmt\n\n* edges have the same problem\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-10-10T11:27:29+01:00",
          "tree_id": "e0093bba97a089239c3ab3dcebcf399fe647ede3",
          "url": "https://github.com/Pometry/Raphtory/commit/c62aa568b2acfdcbd9a87b23dc2179eb4980d417"
        },
        "date": 1760094225180,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1474,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 183,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 170,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1166,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 998,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1527,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "a466dba1400e6784fa42a24670c396ac0ae6f5ae",
          "message": "Separate thread pools for reading and writing in graphql (#2326)\n\n* bring back the write pool to avoid deadlocks due to read and write-locked tasks in the same pool\n\n* fmt\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-10-10T14:49:45+02:00",
          "tree_id": "f766bc3add42316dfc74b5d9642322b15de462d8",
          "url": "https://github.com/Pometry/Raphtory/commit/a466dba1400e6784fa42a24670c396ac0ae6f5ae"
        },
        "date": 1760102759085,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1482,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 213,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 149,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1245,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 972,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1410,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "43c2835d9e1c4202417572d10124dc3a54a6697e",
          "message": "Migrate polars-arrow to arrow-rs (#2316)\n\n* can read and write arrow_rs ChunkedArray\n\n* progress with moving to arrow-rs\n\n* update submodule\n\n* test strings in df loaders\n\n* remove polars from parquet and df loading\n\n* add batch_size argument for the low-level functions\n\n* replace all quickcheck with proptest\n\n* add is_empty check for df loaders\n\n* remove polars as a dependency\n\n* update arrow and pyo3\n\n* more fixed tests\n\n* storage compiles\n\n* raphtory compiles\n\n* start working on cypher\n\n* update submodule\n\n* fix cypher\n\n* everything compiles but some tests are failing\n\n* update submodule\n\n* fix the transpiler test failures due to Join vs Inner Join (they are the same as Inner is the default)\n\n* fix storage feature\n\n* don't add the id column to the schema twice\n\n* fix merge issues\n\n* make node type iterator sized to fix conversion to array\n\n* update submodule\n\n* update submodule\n\n* fix bool and decimal property conversions to array\n\n* time column renamed?\n\n* tidy\n\n* tweak the array export for properties to return pyarrow arrays again\n\n* chore: apply tidy-public auto-fixes\n\n* tidy up warnings\n\n* chore: apply tidy-public auto-fixes\n\n* fixes from running on large graphs\n\n* update submodule\n\n* chore: apply tidy-public auto-fixes\n\n* update submodule\n\n* tidy up the versioning\n\n* tidy up the internal dependencies\n\n* chore: apply tidy-public auto-fixes\n\n* tidy up arrow imports\n\n* fix some imports\n\n* add the missing imports\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: Fabian Murariu <murariu.fabian@gmail.com>\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-10-10T17:21:03+01:00",
          "tree_id": "bf16746403a74d60b08ec64f6d19cdb80f57931c",
          "url": "https://github.com/Pometry/Raphtory/commit/43c2835d9e1c4202417572d10124dc3a54a6697e"
        },
        "date": 1760115423702,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1053,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 117,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 141,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 884,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 724,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1094,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "79378897+arienandalibi@users.noreply.github.com",
            "name": "arienandalibi",
            "username": "arienandalibi"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "08685ee92f4435e9aaadf9f4bb583c40adf707c5",
          "message": "Rolling and expanding window alignment based on the user's time interval input (#2277)\n\n* Added expanding_aligned() and rolling_aligned() functions which behave like expanding() and rolling() windows but are aligned at the start. They get aligned at the smallest unit of time passed as input.\n\n* Cleaned up nested match statements in GraphQL rolling() and expanding() functions. Added align_start flag in those functions as well. Added python tests for alignment of rolling and expanding windows, both for Python and GraphQL. Changed logic so alignment also happens on step if it is provided.\n\n* Fixed python tests for rolling() and expanding(). GraphQL can now take mismatched discrete/temporal intervals for window and step. Tests updated to reflect that.\n\n* Added python tests for rolling() and expanding() functions on different types, such as node, nodes, edge, edges, path_from_node, path_from_graph, and for mismatched window and step types.\n\n* Updated rolling_aligned to only align on the step, not the window (if a step is not passed, then the step defaults to the window). Adjusted tests accordingly. Update window() documentation to not say start and end are optional\n\n* chore: apply tidy-public auto-fixes\n\n* Updated rolling() documentation to indicate that a step larger than window can lead to entries being outside of all windows (before start and/or after end). Updated rolling/expanding tests to verify the last window as well (test boundaries). Added some tests for different step/window combinations.\n\n* chore: apply tidy-public auto-fixes\n\n* Updated rolling() and expanding() to do alignment by default, and rolling_aligned()/expanding_aligned() now take an AlignmentUnit parameter for custom alignment.\n\n* chore: apply tidy-public auto-fixes\n\n* Updated GraphQL tests so that boundaries (last windows) in rolling() and expanding() are checked for all types.\n\n* Updated rolling() and expanding() functions in Python to accept an optional string alignment_unit parameter for custom alignment. If no alignment_unit is passed, aligns on the smallest unit like before. \"unaligned\" allows for no alignment.\n\n* Fixed failing tests\n\n* chore: apply tidy-public auto-fixes\n\n* Updated rolling() and expanding() functions in GraphQL to accept an optional enum alignment_unit parameter for custom alignment, like in Python. If no alignment_unit is passed, aligns on the smallest unit like before. \"unaligned\" allows for no alignment.\n\n* Added tests for window alignment on custom units\n\n* Added tests where windows are weird due to a combination of different window, step, and alignment.\n\n* chore: apply tidy-public auto-fixes\n\n* Added output for weird rolling window alignments in their respective tests\n\n* chore: apply tidy-public auto-fixes\n\n* fixed test for layer filtering no longer affecting the windows created (it previously did). If we do `g.layer(\"x\").rolling(\"1 month\")`, the first window will start aligned with the graph's first event, not the first event on layer \"x\" (even if the first window will be empty)\n\n* Updated rolling windows so that multiples of step are added instead of continuous addition. This fixes the previous bug caused by: Jan 31st + \"1 month\" = Feb 28th; if this happens on the window's end, all following windows will be on the 28th (instead of 29th, 30th, or 31st). This also used to happen for windows always ending on the 30th even if they should end on the 31st. Added and fixed tests for it as well.\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-10-15T15:54:37+01:00",
          "tree_id": "a8f5829448b3bacc39a9ea27eff028a1b25de17c",
          "url": "https://github.com/Pometry/Raphtory/commit/08685ee92f4435e9aaadf9f4bb583c40adf707c5"
        },
        "date": 1760542220353,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1083,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 151,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 165,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 931,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 715,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1129,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "2404621+fabianmurariu@users.noreply.github.com",
            "name": "Fabian Murariu",
            "username": "fabianmurariu"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8a885424ebf1e5e3b86959b5759e9c09caeeff76",
          "message": "update pometry storage and fix the GID column issue (#2332)\n\n* update pometry storage\n\n* update pometry storage",
          "timestamp": "2025-10-16T14:49:28+01:00",
          "tree_id": "1f19410a1b4a83d668abe23569d6cb8ca748c335",
          "url": "https://github.com/Pometry/Raphtory/commit/8a885424ebf1e5e3b86959b5759e9c09caeeff76"
        },
        "date": 1760624739399,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1074,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 123,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 127,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 808,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 688,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1030,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "2404621+fabianmurariu@users.noreply.github.com",
            "name": "Fabian Murariu",
            "username": "fabianmurariu"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c2998a27fe93405289c9bf7f2b380979a4c8ace1",
          "message": "make all the main write locks loopy (#2340)\n\n* make all the main write locks loopy\n\n* fix GqlEdges\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-10-16T15:36:49+01:00",
          "tree_id": "25a9b87e159e6bbb7b170e1ca3997b23c49c39f7",
          "url": "https://github.com/Pometry/Raphtory/commit/c2998a27fe93405289c9bf7f2b380979a4c8ace1"
        },
        "date": 1760627603208,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1047,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 178,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 154,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 771,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 673,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1066,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1c66df1ae1e898173a04d9593c2e1d125f247cc5",
          "message": "Stress tests (#2317)\n\n* got to reproduce the deadlock against 16.1 with different kind of queries besides\n\n* this deadlocks 0.16.1-python and crashes with a panic in /app/raphtory-core/src/storage/mod.rs:881:33 semaphore-plus-final-fix-plus-rwlock-plus-loop-python with 8GB ram\n\n* fixes some queries and also reproduces the deadlock against semaphore-plus-final-fix-plus-rwlock-plus-loop-python\n\n* this version reproduces two different types of panics depending on which one nodes/edges is enabled\n\n* some commented out to get a reduced versions of the read query causing the deadlock\n\n* add parameter to all the decisions\n\n* stress test until error\n\n* add debug.Dockerfile\n\n* add stress tests to github ci\n\n* stress test was missing in the right workflow\n\n* revert back changes on edges.rs\n\n* change stress test parameters\n\n* run raphtory as bg task\n\n* reduce the number of vus to 400\n\n* reduce the number of vus to 300 and upload report\n\n* clean up Makefile\n\n* revert back version for private storage\n\n* decrease vus and remove comments\n\n* always pulaod stress test report\n\n* reduce vus to 10\n\n* fix report missing\n\n* add a name for the stress test report\n\n* chore: apply tidy-public auto-fixes\n\n* address Ben comments\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-10-17T14:33:51+02:00",
          "tree_id": "47fa03fb733fef77782767bde756e64521852e06",
          "url": "https://github.com/Pometry/Raphtory/commit/1c66df1ae1e898173a04d9593c2e1d125f247cc5"
        },
        "date": 1760706548982,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1330,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 164,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 173,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1070,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 908,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1307,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "james.baross@pometry.com",
            "name": "James Baross",
            "username": "jbaross-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "46e0552e304a33ab595658834cc5253d424c6fa5",
          "message": "Explicitly add filter to return types and misc filter stub fixes (#2330)\n\n* explicitly add filter to return types and misc filter stub fixes\n\n* chore: apply tidy-public auto-fixes\n\n* update descriptions of fuzzy search args\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-10-17T17:19:07+01:00",
          "tree_id": "cd2bbcc2ca413b392fce83ebba65790968eb8e01",
          "url": "https://github.com/Pometry/Raphtory/commit/46e0552e304a33ab595658834cc5253d424c6fa5"
        },
        "date": 1760719972515,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1255,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 158,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 166,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 899,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 876,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1140,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "41898282+github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "fa389cb440c0952fe7f3e2b39301b79ced8215da",
          "message": "Release v0.16.3 (#2345)\n\nchore: Release\n\nCo-authored-by: Pometry-Team <ben.steer@pometry.com>",
          "timestamp": "2025-10-21T11:18:52+02:00",
          "tree_id": "5fdb54af0333d8db2b41920a2a44352024ed1371",
          "url": "https://github.com/Pometry/Raphtory/commit/fa389cb440c0952fe7f3e2b39301b79ced8215da"
        },
        "date": 1761040464053,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1240,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 189,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 157,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1077,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 845,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1131,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "james.baross@pometry.com",
            "name": "James Baross",
            "username": "jbaross-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "5384179b492b80ea6bcdc66e9b386d0c8d0f354c",
          "message": "Add how to cite instructions (#2344)\n\n* add how to cite instructions\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-10-21T16:35:54+01:00",
          "tree_id": "433984c282dd6192c2a9080981a17d1c51d812f9",
          "url": "https://github.com/Pometry/Raphtory/commit/5384179b492b80ea6bcdc66e9b386d0c8d0f354c"
        },
        "date": 1761062993765,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1281,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 178,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 138,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1063,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 815,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1264,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "794824c13ff90f9aba737c676040747166aa7a17",
          "message": "Fix master tests (#2348)\n\n* test\n\n* chore: apply tidy-public auto-fixes\n\n* load edges test fails when inputs have the same timestamps, just leave assert_graph_equals\n\n* fix the failing graph load tests\n\n* fix more of the failing df_loaders.rs tests\n\n* chore: apply tidy-public auto-fixes\n\n* Delete stub_gen/build/lib/stub_gen.py\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>\nCo-authored-by: Fabian Murariu <murariu.fabian@gmail.com>\nCo-authored-by: Fabian Murariu <2404621+fabianmurariu@users.noreply.github.com>\nCo-authored-by: Pedro Rico Pinazo <ricopinazo@gmail.com>",
          "timestamp": "2025-10-22T15:57:59+02:00",
          "tree_id": "52dd24959306eb2db2eb3896cd4080723d80b9b0",
          "url": "https://github.com/Pometry/Raphtory/commit/794824c13ff90f9aba737c676040747166aa7a17"
        },
        "date": 1761143479132,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1260,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 181,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 153,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1049,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 851,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1322,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "6fb00f9d8eee3675b604582a29f5519ace0f782a",
          "message": "fix rust release (#2352)\n\n* fix raphtory self dependency and release workflow\n\n* use workspace publish\n\n* update description\n\n* update other descriptions\n\n* add explanatory comment to raphtory self-dependency\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-10-22T17:38:31+02:00",
          "tree_id": "01268a65afabe02e39f45717ac79d26df16bc103",
          "url": "https://github.com/Pometry/Raphtory/commit/6fb00f9d8eee3675b604582a29f5519ace0f782a"
        },
        "date": 1761149517630,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1300,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 175,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 159,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1087,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 705,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1111,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "a6f45782d1bd9374b213d2e75c95570bf08cd2aa",
          "message": "allow partial rust releases (#2354)\n\n* allow partial rust releases\n\n* chore: apply tidy-public auto-fixes\n\n* make rust release both workflow call and workflow dispatch\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-10-23T10:45:28+02:00",
          "tree_id": "87d8081c659e5e0552de33cc271d6c55ba791618",
          "url": "https://github.com/Pometry/Raphtory/commit/a6f45782d1bd9374b213d2e75c95570bf08cd2aa"
        },
        "date": 1761211136019,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1279,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 192,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 158,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1073,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 885,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1237,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "7aad14c75d0982e1a59460974a7faecd1f7daf2a",
          "message": "release workflow cleanup (#2356)\n\n* simplify rust release workflow input\n\n* clean up all release workflows\n\n* fix descriptions for docker build cloud action",
          "timestamp": "2025-10-23T12:45:25+02:00",
          "tree_id": "4715f93ffff874a123c287f3ed2b12f2b5223894",
          "url": "https://github.com/Pometry/Raphtory/commit/7aad14c75d0982e1a59460974a7faecd1f7daf2a"
        },
        "date": 1761218334964,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1296,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 179,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 168,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1086,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 925,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1314,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "675dfd2ad479c8c513c76b358243b929ba9e8b00",
          "message": "grafana example (#2349)\n\n* some changes to the otlp config\n\n* add example for grafana\n\n* add template variable for datasource\n\n* add template variable for service name\n\n* set tempo to allow week long queries\n\n* add duration panels and units\n\n* stick to default json format\n\n* change panel layout\n\n* point raphtory to the latest version\n\n* add a check on otlp_agent_host\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-10-23T16:49:20+01:00",
          "tree_id": "8654537aadff47798318407e7bc1fd2328fc5bbe",
          "url": "https://github.com/Pometry/Raphtory/commit/675dfd2ad479c8c513c76b358243b929ba9e8b00"
        },
        "date": 1761236556808,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1314,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 195,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 184,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1011,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 877,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1266,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "25484244+rachchan@users.noreply.github.com",
            "name": "Rachel Chan",
            "username": "rachchan"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1beacfb63f15a925b8adc9c0aa07d4d554190639",
          "message": "Update UI test commit id (#2366)\n\nupdate ui test commit id",
          "timestamp": "2025-10-30T11:23:33Z",
          "tree_id": "ef375d4b7c0857ac890e6ca4a45e1b46d669a34a",
          "url": "https://github.com/Pometry/Raphtory/commit/1beacfb63f15a925b8adc9c0aa07d4d554190639"
        },
        "date": 1761825529014,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 0,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 194,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 152,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1051,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 854,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1294,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c3b6b387487b72ff05107106a123dc247bf37d61",
          "message": "Update UI to v0.1.18 (#2365)\n\n* Update UI to v0.1.18\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: louisch <772346+louisch@users.noreply.github.com>\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-10-30T12:17:14Z",
          "tree_id": "091d103092479b453f904dfafca242f2e3cc1a7f",
          "url": "https://github.com/Pometry/Raphtory/commit/c3b6b387487b72ff05107106a123dc247bf37d61"
        },
        "date": 1761828658886,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1294,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 175,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 141,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1053,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 864,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1111,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "james.baross@pometry.com",
            "name": "James Baross",
            "username": "jbaross-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "19470cce49edc5e89236192c403ae3ba17a5341b",
          "message": "James/concepts (#2308)\n\n* skeleton\n\n* graph and nodes\n\n* edges\n\n* layers\n\n* Views\n\n* history\n\n* tidy\n\n* chore: apply tidy-public auto-fixes\n\n* typo\n\n* update name\n\n* review comments\n\n* chore: apply tidy-public auto-fixes\n\n* add nodestate mention\n\n* review fixes\n\n* soften update description\n\n* chore: apply tidy-public auto-fixes\n\n* review fixes\n\n* Update 4_key_concepts.md\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>\nCo-authored-by: Ben Steer <b.a.steer@qmul.ac.uk>",
          "timestamp": "2025-11-06T13:17:09Z",
          "tree_id": "defaa56c84709203944566aba192d07ebdd41547",
          "url": "https://github.com/Pometry/Raphtory/commit/19470cce49edc5e89236192c403ae3ba17a5341b"
        },
        "date": 1762437150920,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1303,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 180,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 149,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1001,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 942,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1313,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "3a44116727e270a84bb6ddf2d065973c24ba1018",
          "message": "Updating python version (#2388)\n\nupdating python version",
          "timestamp": "2025-11-20T09:49:43Z",
          "tree_id": "0d76a403429fb672c939995c6de2e907cddf450f",
          "url": "https://github.com/Pometry/Raphtory/commit/3a44116727e270a84bb6ddf2d065973c24ba1018"
        },
        "date": 1763634283163,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1352,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 144,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 139,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1147,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 747,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1345,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "79378897+arienandalibi@users.noreply.github.com",
            "name": "arienandalibi",
            "username": "arienandalibi"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1fddba4669e4375ee8083a6ffac4af78173760ed",
          "message": "Add support for date32, utf8, and different Timestamp(TimeUnit) types when loading from pandas or parquet (#2382)\n\n* Added support for utf8 datetime format for date when loading from pandas. Fixed date32 dtype not being supported when loading from external source like pandas or parquet. Added tests.\n\n* Added support for Date64 and Timstamps of any TimeUnit (Second, Microsecond, Nanosecond).\n\n* Allowed string parsing errors to propagate when loading from pandas/parquet.\nAdded tests for Timstamps of any TimeUnit (Second, Microsecond, Nanosecond).\n\n* chore: apply tidy-public auto-fixes\n\n* Combined the loading tests into one big test\n\n* Combined read_csv tests into one big test\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>\nCo-authored-by: Ben Steer <b.a.steer@qmul.ac.uk>",
          "timestamp": "2025-11-20T16:21:55Z",
          "tree_id": "c95b88606ff59d69c397ed6e96f2dfbd74261b76",
          "url": "https://github.com/Pometry/Raphtory/commit/1fddba4669e4375ee8083a6ffac4af78173760ed"
        },
        "date": 1763657708570,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1210,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 165,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 171,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1057,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 856,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1238,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "395275b6a227bf2eb2bee78674259250cd4c9e68",
          "message": "call blocking_compute in health check (#2392)\n\n* call blocking_compute in health check\n\n* add a test\n\n* address Lucas comments\n\n* set timeout server side",
          "timestamp": "2025-11-24T15:20:26Z",
          "tree_id": "e85e1318866bf9f4e7a5565f365bc38f4e296643",
          "url": "https://github.com/Pometry/Raphtory/commit/395275b6a227bf2eb2bee78674259250cd4c9e68"
        },
        "date": 1763999739960,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1312,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 163,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 160,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 967,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 923,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1298,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "james.baross@pometry.com",
            "name": "James Baross",
            "username": "jbaross-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c8157199165e66b2c86f1601a8e23169abe5f334",
          "message": "add-install-to-get-started (#2389)\n\nMoves installation section under 'getting started' for greater visibility. Adds skeleton of a compatibilities page.",
          "timestamp": "2025-11-26T10:11:15Z",
          "tree_id": "0dd1a598c929f7c9adb69abc407d30b7a5f127d1",
          "url": "https://github.com/Pometry/Raphtory/commit/c8157199165e66b2c86f1601a8e23169abe5f334"
        },
        "date": 1764154016990,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1300,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 184,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 174,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 976,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 679,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1262,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "james.baross@pometry.com",
            "name": "James Baross",
            "username": "jbaross-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "73458309f4648ac41819700ae2ff7829795db28c",
          "message": "Docs/graph styles (#2397)\n\n* initial draft\n\n* full tables\n\n* full tables\n\n* full tables\n\n* reorg pages\n\n* add screenshots\n\n* add screenshots\n\n* typo fix\n\n* fix merge errors",
          "timestamp": "2025-11-26T14:01:59Z",
          "tree_id": "0cf57a577acb9c423dd45df6bc3087f3304f04c2",
          "url": "https://github.com/Pometry/Raphtory/commit/73458309f4648ac41819700ae2ff7829795db28c"
        },
        "date": 1764167846729,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1225,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 169,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 128,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1054,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 909,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1302,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "james.baross@pometry.com",
            "name": "James Baross",
            "username": "jbaross-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ca869cb504ac545e1e348f7671d37240032520ac",
          "message": "James/vectors (#2363)\n\n* fix some docstring formatting\n\n* add vector user guide\n\n* add vector user guide\n\n* add vector user guide\n\n* add vector user guide\n\n* improved docstrings\n\n* clarify document format\n\n* simplest rag-like example\n\n* simplest rag-like example\n\n* fix misc docs errors\n\n* chore: apply tidy-public auto-fixes\n\n* Address review comments\n\n* Add Mini Jinja link\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-11-27T17:15:03Z",
          "tree_id": "73475ac5d136600ab17551da773caa5954866475",
          "url": "https://github.com/Pometry/Raphtory/commit/ca869cb504ac545e1e348f7671d37240032520ac"
        },
        "date": 1764265824171,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1282,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 180,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 179,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1096,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 851,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1108,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "48d12252324210befab645ff844f77b730a2e8de",
          "message": "Added new tracing levels (#2400)\n\n* Added new tracing levels\n\n* fix test\n\n* Added new levels and changed to enum\n\n* return early is server task fnishes\n\n* Docs\n\n* break out tracing in doc\n\n---------\n\nCo-authored-by: Pedro Rico Pinazo <ricopinazo@gmail.com>\nCo-authored-by: James Baross <james.baross@pometry.com>",
          "timestamp": "2025-12-01T22:59:09Z",
          "tree_id": "c964e94a5e040bbd2d132e8e7ea6515d467e67ef",
          "url": "https://github.com/Pometry/Raphtory/commit/48d12252324210befab645ff844f77b730a2e8de"
        },
        "date": 1764632066826,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1260,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 165,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 155,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1066,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 905,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1205,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "louisch@fastmail.com",
            "name": "Louis Chan",
            "username": "louisch"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e386e7ee7e86df57b88f2999faf1af85a36b859e",
          "message": "Fix requests for the worker in subpaths (#2408)",
          "timestamp": "2025-12-03T17:44:41Z",
          "tree_id": "29266aa73ce9dfcbeea5a7a0f297fb129bd908e0",
          "url": "https://github.com/Pometry/Raphtory/commit/e386e7ee7e86df57b88f2999faf1af85a36b859e"
        },
        "date": 1764785995948,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1265,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 186,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 174,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1090,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 849,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1330,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "49699333+dependabot[bot]@users.noreply.github.com",
            "name": "dependabot[bot]",
            "username": "dependabot[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "12b1bc7ccfc7e79c0ef118c8dc17cbc2c75a43a7",
          "message": "Build(deps): bump jws from 4.0.0 to 4.0.1 in /graphql-bench in the npm_and_yarn group across 1 directory (#2410)\n\nBuild(deps): bump jws\n\nBumps the npm_and_yarn group with 1 update in the /graphql-bench directory: [jws](https://github.com/brianloveswords/node-jws).\n\n\nUpdates `jws` from 4.0.0 to 4.0.1\n- [Release notes](https://github.com/brianloveswords/node-jws/releases)\n- [Changelog](https://github.com/auth0/node-jws/blob/master/CHANGELOG.md)\n- [Commits](https://github.com/brianloveswords/node-jws/compare/v4.0.0...v4.0.1)\n\n---\nupdated-dependencies:\n- dependency-name: jws\n  dependency-version: 4.0.1\n  dependency-type: indirect\n  dependency-group: npm_and_yarn\n...\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>",
          "timestamp": "2025-12-04T20:41:39Z",
          "tree_id": "e1b0d79fe5625ab1a0dceaa1bf79a415511bd502",
          "url": "https://github.com/Pometry/Raphtory/commit/12b1bc7ccfc7e79c0ef118c8dc17cbc2c75a43a7"
        },
        "date": 1764883030857,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1291,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 172,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 157,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1088,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 804,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1244,
            "unit": "req/s"
          }
        ]
      }
    ]
  }
}