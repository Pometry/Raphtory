window.BENCHMARK_DATA = {
  "lastUpdate": 1771434183429,
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
          "id": "bfe42154cecafa13441f597c1821a2a91d4c29ad",
          "message": "example metadata (#2398)\n\n* example metadata\n\n* update style schema\n\n* fix formatting\n\n* Update 20_graph_view.md\n\n---------\n\nCo-authored-by: Ben Steer <b.a.steer@qmul.ac.uk>",
          "timestamp": "2025-12-10T08:42:14-08:00",
          "tree_id": "7512fc1b118239db84363acd178f7a79fc6a0ddf",
          "url": "https://github.com/Pometry/Raphtory/commit/bfe42154cecafa13441f597c1821a2a91d4c29ad"
        },
        "date": 1765387048820,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1283,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 170,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 141,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 886,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 826,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1277,
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
          "id": "378f872671a289fc5fa20720479aee43f63e05e5",
          "message": "fix error with 14.1 python tests (#2414)\n\n* fix error with 14.1 python\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-12-12T11:04:08Z",
          "tree_id": "156f728693ec4a83436dfab41d7113cf8d693b44",
          "url": "https://github.com/Pometry/Raphtory/commit/378f872671a289fc5fa20720479aee43f63e05e5"
        },
        "date": 1765539567512,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1260,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 174,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 163,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1078,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 906,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1311,
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
          "id": "7e5a1e65cc4ef0e7497b007d43bbbb04e9b3688e",
          "message": "Release v0.16.4 (#2417)\n\nchore: Release\n\nCo-authored-by: Pometry-Team <ben.steer@pometry.com>",
          "timestamp": "2025-12-12T13:53:32Z",
          "tree_id": "1fdbb76bba9463f0d5afd73c3a6ce10f2300f42c",
          "url": "https://github.com/Pometry/Raphtory/commit/7e5a1e65cc4ef0e7497b007d43bbbb04e9b3688e"
        },
        "date": 1765549634297,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1263,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 191,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 167,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1093,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 893,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1295,
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
          "id": "9fd87709d3865c1dd060a5167951716dc390a97d",
          "message": "Adding support for loading data from any `__arrow_c_stream__` source (#2391)\n\n* Added tests for loading edges from polars and from fireducks. Added a load_edges_from_polars that internally calls to_pandas() on the polars dataframe. Fireducks works.\n\n* Adding loading of data (only edges for now) from arrow directly\n\n* Adding loading of data (only edges for now) from arrow with streaming in rust instead of obtaining each column of each batch from Python individually.\n\n* Added loading of edges from DuckDB, either normally or using streaming.\n\n* Added loading edges from fireducks.pandas dataframes. General cleaning up. Committing benchmarks and tests that check graph equality when using different ingestion pathways.\n\n* Adding flag to stream/not stream data in load_* functions. Will get rid of them and always stream. Added benchmark for loading from fireducks.\n\n* Added functions for load_nodes, load_node_props, load_edges, load_edge_props, that all use the __arrow_c_stream__() interface. If a data source is passed with no __len__ function, we calculate the len ourselves. Updated ingestion benchmarks to also test pandas_streaming, fireducks_streaming, polars_streaming\n\n* Cleaned up benchmark print statements\n\n* Ran make stubs\n\n* Removed num_rows from DFView. No longer calculating/storing the total number of rows.\n\n* Cleaned up load_*_from_df functions. load_edge_props/load_node_props renamed to load_edge_metadata/load_node_metadata.\n\n* Re-added total number of rows in DFView, but as an Option. We use it if the data source provides __len__(), and if not, the loading/progress bar for loading nodes and edges doesn't show progression, only iterations per second.\n\n* Added splitting of large chunks into smaller chunks so that the progress bar for loading updates properly when using the __arrow_c_stream__ interface.\n\n* Renamed props to metadata for remaining functions\n\n* Added tests to check equality between graphs created using different ingestion pathways\n\n* Changed load_*_metadata_* back to load_*_props_*\n\n* Fixed tests and updated workflow dependencies\n\n* Added try-catch blocks for fireducks import in tests\n\n* Fixed tests and notebooks\n\n* Fixed invalid function call in test\n\n* Fixed fireducks package not available on Windows (for now anyway)\n\n* Added load_*_from_df functions to PyPersistentGraph, including load_edge_deletions_from_df\n\n* Cleaned up load_from_df tests and parametrized them to run for both event graphs/persistent graphs.\n\n* Fixed bug in tests\n\n* Removed btc dataset benchmarks\n\n* Merge cleanup and fixing python docs errors\n\n* Changed unsafe ArrowArrayStreamReader pointer cast to stream arrow data from python. Replaced it with PyRecordBatchReader::from_arrow_pycapsule for safety and future changes.\n\n* Removed artefact comment\n\n* Fix innit and tidy\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: miratepuffin <b.a.steer@qmul.ac.uk>\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2025-12-12T15:23:23Z",
          "tree_id": "4e586b033864158a919058827769463a3019d8ab",
          "url": "https://github.com/Pometry/Raphtory/commit/9fd87709d3865c1dd060a5167951716dc390a97d"
        },
        "date": 1765555151310,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1242,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 193,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 162,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1094,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 904,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1231,
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
          "id": "b01b962878085f67973c3c58ca56e81485b23f87",
          "message": "Distinct History Object for nodes and edges (#2075)\n\n* Implementing HistoryObject for node and edge objects. Currently, the HistoryObject holds iterators over history items to return them as needed. HistoryObjects can be aggregated in CompositeHistory.\n\n* Updated HistoryObject to now be called HistoryImplemented, and HistoryObject is now a trait. HistoryImplemented holds a node or edge object and retrieves forward and reverse iterators through functions. CompositeHistory now holds a vector of Boxes containing HistoryObjects.\n\n* Updated HistoryImplemented to have merge() and compose() functions. They return MergedHistory and CompositeHistory wrapped in HistoryImplemented objects respectively. MergedHistory holds two InternalHistoryOps objects while CompositeHistory stores a vector of arbitrarily many. MergedHistory is slightly more efficient but if nested with itself, can lose efficiency.\n\n* Updated history object. Began implementing the pyo3 interface. The pyo3 file doesn't compile yet\n\n* Implemented preliminary PyO3 file which currently compiles. We can get an iterator over the history events in the history object. History objects can be created from nodes and edges. We can do list(history_object) in python. Removed Arc constraint on the history object.\n\n* Added PartialEq and Eq to History Objects, which compare their iterators. Attempted to implement __eq__ and __ne__ for python objects. Added timeindex.rs file to raphtory-api module to support the conversion of TimeIndexEntry type into PyRaphtoryTime, accessible in python.\n\n* History object is now fully accessible in python. Merge and composite functions still need to be added\n\n* Added compose_from_histories() function.\n\n* CompositeHistory object now uses Arc instead of Box as its pointers. Started work on python compose_from_items() function, doesn't work yet. Can't retrieve the python objects. Implemented first iteration of graphql file\n\n* Nodes and Edges return the history object when calling history on them in graphql. Implemented hashing both in rust and in python. They can theoretically now be used in hashmaps (maps) in python.\n\n* Implemented tests for History object in graphql using python. Tested directly on nodes and edges, using windows, using layers, and using property filters.\n\n* Renamed TemporalProp to TemporalProperty\n\n* Synced with master and ran rustfmt\n\n* Made TimeIndexEntry available in Python so that they can be created and manipulated. They implement Eq and Ord functionalities, even in python\n\n* Ran rustfmt\n\n* Trying to change NodeOp for History to return the history object rather than a vec<i64>\n\n* Renamed the history operation from History to HistoryOp, the history object is called History. Fixed implementation of NodeOp for HistoryOp.\n\n* Updated the rest of the code to work with new history object being returned as a result of applying HistoryOp\n\n* Implemented InternalHistoryOps for TemporalProperties\n\n* Changed InternalHistoryOps implementation from TemporalProperties to TemporalPropertyView and ran rustfmt\n\n* Fixed some python errors regarding the history object\n\n* Updated TimeSemantics node_earliest_time() and node_latest_time() to return TimeIndexEntry instead of i64. Created new EarliestDateTime struct which implements NodeOp but returns DateTime<Utc> instead of i64, might be removed later. Removed NodeOpFilter from Map for NodeOp\n\n* All tests pass using the history object\n\n* Implemented error type TimeError for errors related to time operations. Currently supports OutOfRange when trying to convert to DateTime<Utc> from an out-of-range i64 and NotFound for operations where a time entry is not found. Changed output type of dt() from an Option to a Result so that these errors can be propagated.\n\n* Changed most TimeErrors for GraphErrors for consistency. Went back to Options for time information which isn't found, now Result<Option<DateTime>, GraphError>. Continued integrating these in the python interface. Errors in macros persist.\n\n* Made TemporalPropertyView Clone. Changed it's history function to return my history object. Changed function temporal_history() of TemporalPropertyViewOps to return TimeIndexEntries instead of i64. Propagated changes\n\n* Fixed python interface for temporal props\n\n* Attempted to fix errors in lazy_node_state macros, doesn't work. We need to handle Results differently. We might need a new Op type that can return errors so they can be handled properly, especially in the macros.\n\n* Implemented Intervals object so that we can work on intervals between timestamps\n\n* Changing datetime functions to return Result<Option, Error> because dt conversion could fail\n\n* reformat using rustfmt\n\n* History Object no longer has errors, implemented iter and iter_rev for node_view and edge_view. iter_rev does not apply TimeSemantics filtering for edge_view yet.\n\n* Changed HistoryOp to return the history object instead of Vec<i64>. HistoryOp on nodes now has a lifetime because NodeView has a lifetime and is contained in the output History Object.\n\n* Added ops::EarliestTime/ops::LatestTime implementations to history object as comments in case we want to use them instead. Fixed tests because they were failing after the refactor\n\n* Changed many time operations (such as history, earliest_time, latest_time, ...) to return TimeIndexEntry for nodes, edges, and temporal props. Propagated these TimeIndexEntries.\n\n* Implemented _rev functions that return iterators over edge histories in reverse, both for edge_history and edge_history_window functions. Updated history object to use these new functions\n\n* Implemented _rev functions that return iterators over node histories in reverse, both for node_history and node_history_window functions. Updated history object to use these new functions\n\n* Created HistoryRef which holds references to items instead of owning them to fix lifetime issues. Fixed many errors in Python module. Propagated TimeIndexEntries in many functions, especially relating to props. Added equality comparison on History object and on TimeIndexEntry==i64.\n\n* Updated intervals to hold an item which yields time entries instead of holding a vector of interval values. The interval values can be calculated lazily.\n\n* Updated intervals to hold a reference to an item which yields time entries instead of owning the item. Updated tests for intervals and for the history object when using a layer.\n\n* Changed EdgeViewOps history() function to return History object instead of a vector of all timestamps\n\n* Added t(), dt(), and s() functions to History object. They return a history object whose iter() and collect() functions yield items of the appropriate type (i64 timestamp, DateTime<Utc>, or secondary index). They all own the InternalHistoryOps object they use (by receiving a clone), same in intervals and merge. That is, except in HistoryRef, where they receive a reference. Added len function to InternalHistoryOps. Clone requirement added throughout History.rs.\n\n* Added history() function to Nodes struct which returns History object. Fixed some History earliest_time and latest_time implementations in aggregate types to call respective inner time functions. CompositeHistory is now generic over lifetimes (namely NodeViews with 'graph lifetime).\n\n* Changed EdgeView time functions to return TimeIndexEntry instead of i64. History object's earliest_time and latest_time can now be done more efficiently by calling the edge's appropriate functions. History objects are now generic over lifetimes\n\n* Implemented history functions directly on LazyNodeState<HistoryOp...>. They map each node to a History object containing it. Wrote test cases for graph.nodes().history(), which returns a history object per node, and for History::new(LazyNodeState<HistoryOp...>), which returns a single history object for all nodes. The latter orders all time entries for the whole set of nodes, while the former only orders time entries within each node.\n\n* Added support for graph.nodes().neighbours().combined_history() and graph.node(\"some_node\").neighbours().combined_history() by implementing InternalHistoryOps for PathFromNode and PathFromGraph.\n\n* Working on macros so that History object can be implemented into python. Created some new types for Python iterables, especially for edges. Started working on macros for node ops as well. Still unfinished.\n\n* Added Nested types on Result iterables types for Python. Fixed all Edges functions in Python. PyGenericIterator and PyNestedGenericIterator have \"from_result_iter\" and \"from_nested_result_iter\" constructors to support result types.\n\n* Changed PyEdge and PyNode history() functions to return history object. Added HistoryTimestamp, HistoryDateTime, and HistorySecondary in Python. Fixed PyPathFromGraph iterable return types. Fixed errors\n\n* Added t, dt, s, intervals functions in python. Changed __list__ and collect_rev functions to return NumPy arrays instead of Vec, better for performance. Fixed some history functions in python to return proper history objects.\n\n* Fixed PyBorrowingIterator for Result types. Created new macro and function to create them for iterators that return Result types which need to be converted into python objects\n\n* Removed _date_time functions (earliest_date_time, latest_date_time, history_date_time, ...), as well as history_counts functions. Added support for history.reverse() which reverses iter() and iter_rev() functions. Added combined_history() functions for PathFromGraph and PathFromNode\n\n* Removed histories, histories_rev, histories_date_time from TemporalPropertyView. Changed graph.nodes().start() and end() to return TimeIndexEntry instead of i64. WindowedGraph now holds Option<TimeIndexEntry> instead of Option<i64> as start and end. Changed view_start(), view_end(), shrink_start(), shrink_end(), shrink_window(). Changed internal_window() to take Option<TimeIndexEntry> for window creation. Changed impl_timeops! macro.\n\n* Changed timeline_start and timeline_end to return Option<TimeIndexEntry> instead of Option<i64>. Fixed tests.\n\n* Changed IntoTime to return TimeIndexEntry instead of i64. Added AsTimeInput trait for time inputs. Added saturating_add on TimeIndexEntry which increments the timestamp and leaves secondary index untouched\n\n* Fixed some errors after merge. Changed node_earliest_time_window and node_earliest_time_window to return TimeIndexEntry instead of i64. Removed unused imports\n\n* Changed windowing functions to use TimeIndexEntry instead of i64, especially in the input Range: has_temporal_prop_window, temporal_prop_iter_window, window_bound, is_node_prop_update_available_window, is_node_prop_update_latest_window, is_edge_prop_update_available_window, is_edge_prop_update_latest_window. Removed temporal_history_date_time, items_date_time.\n\n* Moved IntoTime (and associated traits), as well as PyTime to raphtory-api crate. Moved FromPyObject implementation which parses time/date inputs to TimeIndexEntry instead of PyTime. This is now in raphtory-api. PyRaphtoryTime has been replaced by PyTime.\n\n* Updated all function arguments which were previously PyTime to now be TimeIndexEntry because we changed the parsing of input times from PyTime to TimeIndexEntry.\n\n* Current progress in implementing Python versions of LazyNodeState and NodeState for EarliestDateTime NodeOp. Added macro to build py_borrowing_iter where the item is a Tuple containing a result. Manually implementing the python structs so that we can handle the Result types appropriately.\n\n* Implemented Python wrapper for NodeState of Result<Option<DateTime<Utc>>, TimeError> types. Has group_by ops as well as ord ops. Error values and None values are ignored in many functions, such as median and group_by operations.\n\n* Implemented Python wrapper for LazyNodeState of Map<EarliestTime, Result<Option<DateTime<Utc>>, TimeError>> for EarliestTime and LatestTime types. Has group_by ops as well as ord ops. Error values and None values are ignored in many functions, such as median and group_by operations. Still missing layer ops and time ops, we need to implement NodeOpFilter for Map.\n\n* Re-introduced NodeOpFilter for Map so that OneHopFilter operations can be accessed on LazyNodeState<Map<...>>. One hop implemented on EarliestDateTimeView and LatestDateTimeView.\n\n* Implemented Ord and PartialOrd for History objects. Implemented FromPyObject for History<Arc<dyn InternalHistoryOps>>. Implemented OneHopFilter for LazyNodeState<HistoryOp>. Implemented HistoryView, a python wrapper for LazyNodeState<HistoryOp>. Implemented a python wrapper for NodeState<History<NodeView>>. Commented out EarliestDateTimeView, LatestDateTimeView, and HistoryDateTime python wrappers for respective ops. Core raphtory now compiles.\n\n* Removed Ord and PartialOrd implementations for History object, they don't make sense. Updated PyHistory to include more functions.\n\n* Fixed GraphQL errors. Added more functions to GqlHistory available in Rust.\n\n* Added GqlTimeIndexEntry, a GraphQL wrapper for TimeIndexEntry. Time operations now return this wrapper. GqlPropertyTuple operations now use this wrapper as well.\n\n* Changed GqlHistory to return GqlTimeIndexEntry instead of i64 for time related functions. Added GqlHistoryTimestamp, GqlHistoryDateTime, GqlHistorySecondaryIndex, which are wrappers around their respective Rust History objects. They provide access to history information in different formats in GraphQL. GqlHistoryDateTime outputs datetimes as formatted strings. Default formatting is RFC 3339 and allows formatting using custom format strings. Uses chrono::format::strftime for formatting.\n\n* Updates to GraphQL history objects: updated all functions to use blocking_compute(). Added page() and page_rev() functions. Renamed collect() and collect_rev() functions to list() and list_rev().\n\n* Fixed errors after merge\n\n* Fixed tests after merge\n\n* Re-added pometry-storage-private submodule at commit fac0f56\n\n* Activated private storage. Moved some Python logic around, moved some Python specific functionality under python feature.\n\n* Changed t, dt, secondary_index, and history functions to not require brackets () when called in python\n\n* Added support for extracting TimeIndexEntry from [int, int] in Python. Like this, the secondary index can be specified in Python. Removed saturating_add() function on TimeIndexEntry because it might be confusing. next() and previous() on TimeIndexEntry should increment the secondary index, not the timestamp.\n\n* Changed TimeIndexEntry parsing from Python to allow tuples/lists (of size 2) using any combination of types already supported. Allow (int, int), (str, int), (int, str), (date, int), (int, date), and so on...\n\n* Fixed the rust tests, they all pass. Parquet serialization tests only check timestamps, secondary indices are not preserved. Update python add functions (add_node, add_edge, add_updates) to take TimeIndexComponent (i64) instead of TimeIndexEntry.\n\n* Updated stubs. Updated stub_gen and makefile to work with conda environments. Updated add_classes! macro used for Python. Renamed RaphtoryTime in Python to TimeIndexEntry. Added t, dt, secondary_index, intervals functions to HistoryIterable (obtained with g.edges.history). Implemented comparison between TimeIndexEntry and other time types (i64, str, datetime, other TimeIndexEntry, ...)\n\n* Added t, dt, secondary_index, and intervals functions on NestedHistoryIterable (obtained by something like g.nodes.in_edges.history). Re-added histories() on PyTemporalProperties. Fixed From<&PyTemporalProperties> for PyTemporalPropsCmp to use iter_filtered instead of iter. If a property is empty at that time, it won't include an empty list (fixed python comparisons)\n\n* Added __repr__() for different history types (t, dt, secondary_index, intervals). Updated TimeIndexEntry to pass errors through. If a single component fails to convert (from string or float parsing), the appropriate error message now surfaces to python. Furthermore, each time component in a list/tuple has individual error messages surface to help fixing errors.\n\n* Current progress in fixing the python tests\n\n* Added t, dt, secondary_index on python LazyNodeStates (for earliest and latest times) as well as Python TimeIndexEntry iterables. Fixed some errors related to exporting, networkx, and TemporalProperty history. Added __contains__, __eq__, __ne__ for all history objects (History, HistoryTimestamp, ...)\n\n* Fixed all python tests\n\n* Fixed rust-side t, dt, secondary_index, intervals functions on LazyNodeState<HistoryOp>\n\n* Current progress in resolving issues\n\n* Current progress in resolving issues\n\n* Current progress in resolving issues. Added first() and last() on history objects. Changed median() on intervals to return i64 instead of f64. Removed display() on GqlTimeIndexEntry.\n\n* Added DeletionHistory to History. edge1.deletions() now returns a History<DeletionHistory<EdgeView>> object. Added reverse iterators for edge_deletion_history. Fixed tests.\n\n* Added rust-side t, dt, and secondary_index functions for LazyNodeState<EarliestTime> and <LatestTime>. Added python-side t, dt, secondary_index, intervals, earliest_time, latest_time for LazyNodeState<HistoryOp> and NodeState<History>. They map the Op for LazyNodeState and change computed values for NodeState. Implemented all the different return types in Python (eg. LazyNodeState<ops::Map<HistoryOp, PyHistoryTimestamp>> for all LazyNodeState and NodeState of each History/Interval object. Added collect_valid and iter_valid on LazyNodeState<EarliestDateTime> and <LatestDateTime> which return only valid values (no errors or None)\n\n* Fixed different Time traits in general. Moved FromPyObject for time inputs from TimeIndexEntry to InputTime, which can differentiate between single inputs (timestamp only) and two inputs (secondary index as well).\n\n* Current progress in resolving issues. sorted() and groups() now pull the errors out of the NodeState so that the NodeState doesn't contain any error values. Added values_valid, items_valid, and iter_valid on LazyNodeState<Result<DateTime>> and NodeState<Result<DateTime>> that only return valid values (no errors or None values).\n\n* Added IntoArcDynHistoryOps to define how an InternalHistoryOps object gets wrapped into an Arc<dyn InternalHistoryOps>. Downcasting is no longer used in From<History> for PyHistory to avoid nesting Arcs infinitely.\n\n* Changed WindowTimeSemantics to use a Range<TimeIndexEntry> instead of Range<i64> for it's window. Changed all NodeTimeSemanticsOps and EdgeTimeSemanticsOps to use Range<TimeIndexEntry> as well to reflect this change.\n\n* Got rid of NodeState<Result<Option<DateTime>>>. Any functions that used to return it now return Result<NodeState<Option<DateTime>>>. The function itself might fail but the NodeState will not contain any errors. Changed earliest_time_window and latest_time_window to take TimeIndexEntry instead of i64. Fixed flatten() on LazyNodeState<HistoryOp> and added it on NodeState<History>.\n\n* Changed CompositeHistory to hold Box<[Box<dyn InternalHistoryOps + 'a>]> instead of Vec<Arc<dyn InternalHistoryOps + 'a>>.\n\n* Removed date_time on edge/edges and changed time to return TimeIndexEntry. Added secondary indices to to_networkx() and fixed tests. Added TimeIndexEntryIterable and NestedTimeIndexEntryIterable for edges. Added as_tuple function on TimeIndexEntry.\n\n* Updated GraphQL time-based functions and Windows to take GqlTimeInput (i64, string, or tuple (i64||string, usize) instead of only i64. String is a formatted DateTime string. Changed CompositeHistory to hold Box<[T]>. Updated at(), after(), and before() to ignore secondary index of TimeIndexEntry for the time being.\n\n* Renamed Window in vectorised_graph.rs to VectorisedGraphWindow to avoid name clashing with other Window in GraphQL. Updated tests to use new time input semantics\n\n* Current progress in fixing the docstrings.\n\n* Finished fixing the docstrings.\n\n* Ran tidy-public\n\n* Regenerated python stubs and graphql schema after merge.\n\n* Fixed \"Option\" -> \"Optional\" typo in Python docs\n\n* Removed unused imports, added some GraphQL docs, added some some classes to base_modules.rs for proper type hints.\n\n* Fixed doc tests and storage tests.\n\n* Added python classes to module and fixed iterable return types.\n\n* Fixed some tests. Removed Iterable types from added Python classes, they will be added later.\n\n* chore: apply tidy-public auto-fixes\n\n* Updated some error messages in raphtory-benchmark\n\n* Fixed benchmark\n\n* Fixed merge issues. Added python tests for history object.\n\n* Added tests for checking TimeIndexEntry/InputTime sting parsing, equality, greater than/less than comparisons, tuple behaviour, ...\n\n* chore: apply tidy-public auto-fixes\n\n* Removed histories_timestamps(). Added GraphQL tests for datetimes, formatting using string, secondary index, simple and indexed time input parsing, window borders for indexed time input.\n\n* chore: apply tidy-public auto-fixes\n\n* Allowed history objects to be compared with lists in python. The list can contain anything that can be parsed to a TimeIndexEntry, indexed or not.\n\n* Fixed GraphQL server crashing when invalid format string is passed to datetime function. Updated GqlTimeInput parsing to be simpler. Now, a number, string, or Object {epoch, index} can directly be passed without specifying fields (except for epoch/index to specify index).\n\n* Fixed merge issues\n\n* Fixed failing rust tests\n\n* Fixed failing rust storage tests except for one\n\n* Renaming TimeIndexEntry to EventTime. Updating all documentation, API function names, and associated types (like iterables) to match. EventTime contains a Unix Epoch timestamp and an event_id (previously secondary_index).\n\n* Still renaming TimeIndexEntry to EventTime. Updating all documentation, API function names, and associated types (like iterables) to match.\n\n* Renaming secondary_index from old TimeIndexEntry terminology to event_id.\n\n* Replacing secondary_index terminology from old TimeIndexEntry to event_id in src/search\n\n* Replacing secondary_index terminology from old TimeIndexEntry to event_id for new EventTime.\n\n* Fixed issues\n\n* docs fixes\n\n* update object name to EventTime\n\n* Changed many unnecessary try_into_time() function calls with into_time(). Updated some at() functions to take EventTime/GqlTimeInput instead of i64.\n\n* Fixing tests\n\n* Moved impl blocks for EventTime iterables to a separate file, so custom function definitions aren't alongside macro uses.\n\n* chore: apply tidy-public auto-fixes\n\n* Reverted previous changes made to Makefile\n\n* Cleaned up custom node_state files\n\n* Renamed epoch to timestamp for EventTime\n\n* Fixed tests\n\n* rust format and general merge fixes\n\n* chore: apply tidy-public auto-fixes\n\n* moved history tests to tests folder\n\n* cleaned up docs\n\n* chore: apply tidy-public auto-fixes\n\n* removed changes previously made to stub_gen for debugging purposes\n\n* Added intervals functions such as mean, median, min, max on LazyNodeState<Interval> and NodeState<Interval>. Updated docs. Added to_list() -> list[int] alternatives for all collect() -> np.NDArray[np.int64] functions (PyHistoryTimestamp, PyHistoryEventId, PyIntervals, ...). Reorganized node_state.rs file to remove custom function implements and move them to their own files (node_state_earliest_time.rs, node_state_history.rs, ...). Added NodeState<Option<f64>>. Added a test for LazyNodeState and NodeState of Intervals.\n\n* Fixed issues after merge\n\n* Fixed tests after merge\n\n* chore: apply tidy-public auto-fixes\n\n* Fixed tests after merge\n\n* Updated stubs\n\n* Fixed storage feature imports\n\n* Fixed failing tests and updated earliest_edge_time and latest_edge_time to return EventTime instead of i64\n\n* chore: apply tidy-public auto-fixes\n\n* Fixed failing test\n\n* chore: apply tidy-public auto-fixes\n\n* chore: apply tidy-public auto-fixes\n\n* chore: apply tidy-public auto-fixes\n\n* Adding PyOptionalEventTime, a python wrapper for Option<EventTime> so that functions that return Option<EventTime> don't crash in python when we get None and call .t(), .dt(), ...\n\n* Changing python functions that return Option<EventTime> to return PyOptionalEventTime instead. Calling .t(), .dt(), ... on this object will not raise an exception in python even if the option is None.\n\n* PyOptionalEventTime: added repr() and added it to add_classes!\n\n* Added comparison functions for PyOptionalEventTime. They can be compared with EventTime as normal.\n\n* General fixes and clean up after merge\n\n* Added comparison to PyEventTime for PyOptionalEventTime. Added tests for comparison. Fixed python docs return type annotations.\n\n* Added __getitem__ on history objects to allow subscript notation: history[index]. Added EventTime and OptionalEventTime as TimeInput types. Fixed tests\n\n* Added a GqlOptionalEventTime type so that if a function returns None, we can still call timestamp, datetime, etc... on it without panicking.\n\n* Changed GqlEventTime to hold an Option<EventTime> and got rid of GqlOptionalEventTime. Replaced all uses with this updated type.\n\n* Fixed GraphQL tests\n\n* Added support for ingesting `date` types from python, previously it was just `datetime` types. This is supported anywhere that an EventTime or an InputTime is expected.\n\n* Added IntoIterator for History<T> wrapper objects when T: 'static. Added flatten and flattened_list function for all iterable and nested iterable types of history objects (History, HistoryTimestamp, HistoryDatetime, ...). Added tests for python datetime.date ingestion\n\n* Fixes after merge\n\n* Fixed tests\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>\nCo-authored-by: James Baross <james.baross@pometry.com>\nCo-authored-by: Ben Steer <b.a.steer@qmul.ac.uk>",
          "timestamp": "2025-12-12T12:43:27-05:00",
          "tree_id": "a4a503f2714c9e3549d19421c275dc18cffccd1a",
          "url": "https://github.com/Pometry/Raphtory/commit/b01b962878085f67973c3c58ca56e81485b23f87"
        },
        "date": 1765563561873,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1275,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 177,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 148,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1038,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 773,
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
          "id": "89c09575a68bf6c7b24530f3e3fdf378190f14c8",
          "message": "bring back rust_fmt_check (#2424)",
          "timestamp": "2025-12-16T09:45:54Z",
          "tree_id": "cbf8bba1fcf1d7723b04493dc7ea8761947d69f9",
          "url": "https://github.com/Pometry/Raphtory/commit/89c09575a68bf6c7b24530f3e3fdf378190f14c8"
        },
        "date": 1765880496859,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1256,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 186,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 138,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1079,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 899,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1301,
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
          "id": "102a9fefbe8bdfa045c11d1a4537da6ccefcc293",
          "message": "Make graphql schema output stable (#2427)\n\n* register plugins in stable order to avoid churn in the docs\n\n* clean up warnings",
          "timestamp": "2025-12-19T14:39:26+01:00",
          "tree_id": "a2a6baa98c20e9445d6f5bfd020a5e38afe38082",
          "url": "https://github.com/Pometry/Raphtory/commit/102a9fefbe8bdfa045c11d1a4537da6ccefcc293"
        },
        "date": 1766153741257,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1443,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 174,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 159,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1134,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 765,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1472,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "cheick95@gmail.com",
            "name": "Cheick Ba",
            "username": "BaCk7"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "566bd990611542ef7f58a915878e415671770fb1",
          "message": "local clustering coefficient relax static graph requirement (#2433)\n\n* local clustering coefficient relax static graph requirement\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2026-01-08T16:54:01+01:00",
          "tree_id": "2940e26f163d6f2b71c94c5e0a9757fc4748469d",
          "url": "https://github.com/Pometry/Raphtory/commit/566bd990611542ef7f58a915878e415671770fb1"
        },
        "date": 1767889787942,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1476,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 196,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 154,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1111,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 1016,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1339,
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
          "id": "dff2fc57de039b9982db939fe9708e43a6af0e4e",
          "message": "Update UI to v0.2.1 (#2431)\n\n* Update UI to v0.2.1\n\n* chore: apply tidy-public auto-fixes\n\n* Update action to install all deps\n\n---------\n\nCo-authored-by: louisch <772346+louisch@users.noreply.github.com>\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>\nCo-authored-by: Louis Chan <louisch@fastmail.com>",
          "timestamp": "2026-01-08T19:06:19Z",
          "tree_id": "5d050b3f695623d8d94a4b733e6c3174cfcdbbc2",
          "url": "https://github.com/Pometry/Raphtory/commit/dff2fc57de039b9982db939fe9708e43a6af0e4e"
        },
        "date": 1767901339779,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1437,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 165,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 134,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1208,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 1026,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1503,
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
          "id": "d7678ed7ed41bd9dbddf0a3b67961e6bc4040087",
          "message": "update version of python for the docker image (#2434)\n\n* update version of python for the docker image\n\n* fix syntax error\n\n* tag docker images with the python and debian version used",
          "timestamp": "2026-01-09T14:54:15+01:00",
          "tree_id": "816258df8406c2584aa88a7b7713a73b04cb08dd",
          "url": "https://github.com/Pometry/Raphtory/commit/d7678ed7ed41bd9dbddf0a3b67961e6bc4040087"
        },
        "date": 1767969001257,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1444,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 181,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 119,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1198,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 1019,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1459,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "4599890+shivamka1@users.noreply.github.com",
            "name": "Shivam",
            "username": "shivamka1"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "cd9236aeeb2cd2fb79393dd3bbcb0fd62ba8f058",
          "message": "Master filter (#2254)\n\n* window filter semantics, wip\n\n* fix lifetimes in the eval apis\n\n* add review suggestions\n\n* fix tests\n\n* found some busted bit\n\n* fix type validation for map filters\n\n* improve filter validation\n\n* report correct property name\n\n* clean up the test\n\n* update rust test\n\n* impl one hop filter\n\n* fix tests\n\n* Issues with constant props filter as discussed\n\n* Added some fuzzy tests\n\n* fix tests\n\n* disable constant properties on multilayer edge views\n\n* fix repr for python ConstantProps\n\n* constant properties use layer priority to find the merged value instead of creating a map\n\n* no more enum_dispatch and quickcheck and some other minor tidy up\n\n* update tests (changed semantics for constant properties, no support for comparison operators on list)\n\n* order of layers matters now, need to preserve it in test\n\n* fix py, gql for one hop filter\n\n* simplify LayerColVariants\n\n* no reason to use par_bridge here\n\n* imports\n\n* run tests first before tidy\n\n* ref\n\n* fix tests\n\n* revert cargo\n\n* fix tests\n\n* fix tests\n\n* split properties and metadata\n\n* core bits are compiling, more cleanup needed\n\n* fix tests\n\n* fix python\n\n* bring back map expansion for multilayer metadata view\n\n* everything compiles, some tests to fix\n\n* bring back map validation for metadata of multilayer edges\n\n* start updating tests\n\n* fix python properties keys\n\n* rename constant_properties to metadata\n\n* fix filter module initialisation\n\n* more renaming\n\n* a lot more renaming\n\n* fix metadata filter for exploded edges\n\n* Fixed metadata tests\n\n* fixing semantics tests\n\n* fix semantics\n\n* fixed explode tests\n\n* No_fallback_docs (#2195)\n\n* fix docs tests in ingestion topics\n\n* fix docs tests\n\n* fix docs tests\n\n* fix admonitions\n\n* update storage\n\n* Fixed more filters\n\n* ref, add exploded edge property filtering for py, enable test\n\n* fixed test_graphdb\n\n* fixed load and export tests\n\n* ref search\n\n* fix example\n\n* fix python build\n\n* remove the fallback in search api\n\n* should use filtered iterator in test\n\n* add extension-module to features in makefile\n\n* fix example\n\n* fix example\n\n* fix filter and search tests\n\n* fix the index tests\n\n* fix the old_proto reading test\n\n* fix graphql node schema\n\n* fix the metadata write in the .raph file\n\n* rename the field\n\n* fix imports\n\n* update edge metadata query and test\n\n* properties are now metadata\n\n* use rayon instead of tokio\n\n* fix warning\n\n* one more case of renaming properties to metadata\n\n* fix PyRemotePropertySpec\n\n* update python tests\n\n* fix properties is_empty\n\n* use a TemporaryDirectory for the saving example such that it gets cleaned up afterwards\n\n* Fixed python client and add test for upload graph\n\n* fmt\n\n* fix py import\n\n* fix py test\n\n* fix test\n\n* fmt\n\n* flatten the metadata access in GqlMetaGraph\n\n* add rounding to pagerank example to make the test not flaky\n\n* disable graphql vector test\n\n* bring back filtering of empty/non-existing values for individual Properties and Metadata in python\n\n* bring back filtering of empty/none values in graphql properties/metadata\n\n* no more fallback for the property type in balance\n\n* no more fallback for weight type in dijkstra\n\n* bring the first vector test back\n\n* allow threads in query to avoid gil deadlocks\n\n* bring back the vector tests\n\n* put the ? in the right place\n\n* bring back the extra thread\n\n* revert LayerIds::Mutliple to sorted slice\n\n* needs closure\n\n* update tests for property semantics\n\n* add support for unzipped folders in upload_graph\n\n* release the GIL\n\n* add review suggestions\n\n* add test, ref\n\n* chore: apply tidy-public auto-fixes\n\n* impl #2211\n\n* fmt\n\n* chore: apply tidy-public auto-fixes\n\n* impl temporal first filter\n\n* impl temporal all filter\n\n* public tidy\n\n* impl collectors\n\n* impl list agg filter\n\n* fix agg filter semantics\n\n* impl tests and some fixes\n\n* add agg tests\n\n* add prop python apis to create rust types\n\n* impl python list\n\n* impl python map\n\n* chore: apply tidy-public auto-fixes\n\n* use pytest.raises\n\n* more tests\n\n* fix comment\n\n* fix comment\n\n* simplify props\n\n* fix test\n\n* fix\n\n* fix test\n\n* unique results based search, fix tests\n\n* rid dead collectors\n\n* fix test\n\n* fmt\n\n* chore: apply tidy-public auto-fixes\n\n* impl node py filter iter (#2270)\n\n* impl node py filter iter\n\n* chore: apply tidy-public auto-fixes\n\n* impl path_from_graph, path_from_node, edges, nested_edges py filter, fix tests,\n\n* rid iter_graph\n\n* add more tests\n\n* add more tests\n\n* add exploded edges test\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>\n\n* chore: apply tidy-public auto-fixes\n\n* Features/filter (#2281)\n\n* impl node id filter\n\n* impl node id filter for gql\n\n* impl edge src/dst id filter\n\n* chore: apply tidy-public auto-fixes\n\n* add validations, support numeric, string filters for ids\n\n* rid dead code\n\n* add review changes\n\n* fix options\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>\n\n* rid dead code\n\n* chore: apply tidy-public auto-fixes\n\n* add u8, u16, u32, f32, i32 to the graphql types (#2287)\n\n* chore: apply tidy-public auto-fixes\n\n* chore: apply tidy-public auto-fixes\n\n* impl qualifier filters for list properties and metadata (#2284)\n\n* impl qualifier filters for list properties and metadata\n\n* impl python, graphql qualifier filter, add tests\n\n* rename gql from gql object names, throw error if prop not found\n\n* ref\n\n* add more suggestions\n\n* allow empty graph in tests\n\n* allow empty graphs for tests\n\n* fix test\n\n* chore: apply tidy-public auto-fixes\n\n* impl aggs for temporal properties, add tests (#2294)\n\n* impl aggs for temporal properties, add tests\n\n* add review suggestions\n\n* redesign, refactor property filters (#2311)\n\n* redesign, refactor property filters\n\n* takes a reference\n\n* rework validation\n\n* fix arbitrary list, fix tests\n\n* merge python traits, fix tests, add more validations\n\n* rework gql filtering apis, fix tests\n\n* add review suggestions\n\n* Features/gql apis (#2350)\n\n* impl nodes select filtering in gql\n\n* change semantics of filters in gql, add missing filter apis in edges, fix all tests\n\n* add more edge filter tests\n\n* add filtering to path from node, add tests\n\n* fix apply views\n\n* rename filter-iter to select\n\n* add select as args\n\n* impl window filter (#2359)\n\n* impl window filter\n\n* impl window filter in python, add tests\n\n* impl gql window filter, add tests\n\n* ref\n\n* impl review suggestions\n\n* fixes\n\n* fix py and gql\n\n* add review suggestions\n\n* impl edge node filtering, add few tests (#2364)\n\n* redesign, refactor property filters\n\n* takes a reference\n\n* rework validation\n\n* fix arbitrary list, fix tests\n\n* merge python traits, fix tests, add more validations\n\n* rework gql filtering apis, fix tests\n\n* impl nodes select filtering in gql\n\n* change semantics of filters in gql, add missing filter apis in edges, fix all tests\n\n* add more edge filter tests\n\n* add filtering to path from node, add tests\n\n* impl window filter\n\n* impl window filter in python, add tests\n\n* impl gql window filter, add tests\n\n* ref\n\n* impl edge node filtering, add few tests\n\n* rid redundant code\n\n* fix call to filter nodes\n\n* rid dead code\n\n* Integrating edge endpoint filtering mechanism into Python using the same wrapper types as node filtering.\n\n* Added src/dst endpoint filtering support for exploded edge filters in rust and python\n\n* Added src/dst endpoint filtering support for exploded edge filters in GraphQL and fixed search\n\n* Added tests from previous branch, some of them fail\n\n* Fixed DynFilterOps implementations for EndpointWrapper<T> types. Endpoint filtering tests pass\n\n* Changed many impls to be blanket implementations using traits, especially EndpointWrapper<T> types. Use indexes in search on edge endpoints using NodeFilterExecutor.\n\n* rid dead code\n\n* nodeops suggestions from lucas\n\n* start fixing some apis\n\n* fixed most of the compilation errors\n\n* fix all the python problems except for actually implementing the python filtering\n\n* finish ref, fix tests\n\n* redone\n\n* fix\n\n* more changes\n\n* start fixing infinite trait bound recursion\n\n* rid filtered graphs\n\n* rid nodetypefilteredgraph\n\n* add review suggestions\n\n* fix infinite type recursion\n\n* fmt, fix recursion issue in search\n\n* impl py\n\n* Do not rely on Wrap for the trait bounds in the builder API as the compiler cannot figure out the bounds when trying to implement python wrappers.\n\n* fix gql, tests\n\n---------\n\nCo-authored-by: arienandalibi <arienandalibi2@gmail.com>\nCo-authored-by: Lucas Jeub <lucas.jeub@pometry.com>\n\n* Refactoring filter (#2404)\n\n* ref property filters\n\n* move windowed filter into its own file\n\n* ref node filter\n\n* ref filter module\n\n* move InternalPropertyFilterBuilderOps to mod\n\n* move validation and evaluation logic to independent files\n\n* ref\n\n* simple renames\n\n* rid duplicate temporal impl\n\n* rename endpoint wrappers\n\n* rearrange\n\n* impl node select\n\n* move createfilter to respective files\n\n* ref\n\n* move createfilter impl to respective files, ref\n\n* replace enum with macro, rename trait\n\n* replace edge enums with macro, rename traits\n\n* give sensible names to all types\n\n* add review suggestions\n\n* Layers (#2428)\n\n* layer impl rust, py, add tests\n\n* impl gql layers, add tests\n\n* fix tests\n\n* disable a test for now because node layers not supported yet\n\n* add review suggestions\n\n* merge from master\n\n* fix tests\n\n* fix tests\n\n* update private\n\n* fix stress\n\n* fix test\n\n* fix docs\n\n* fix bug caught by stress test\n\n* chore: apply tidy-public auto-fixes\n\n* limit query depth\n\n* add graph latest bm\n\n* increase the stack size, not a fix though\n\n* fix test, ref\n\n* update ui\n\n* Update test_during_pr.yml\n\n---------\n\nCo-authored-by: Lucas Jeub <lucas.jeub@pometry.com>\nCo-authored-by: Ben Steer <ben.steer@pometry.com>\nCo-authored-by: James Baross <james.baross@pometry.com>\nCo-authored-by: Ben Steer <b.a.steer@qmul.ac.uk>\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>\nCo-authored-by: arienandalibi <arienandalibi2@gmail.com>",
          "timestamp": "2026-01-13T15:50:56Z",
          "tree_id": "9deccd38e0b4ef3dbe1de4c573eac07d43c8bc18",
          "url": "https://github.com/Pometry/Raphtory/commit/cd9236aeeb2cd2fb79393dd3bbcb0fd62ba8f058"
        },
        "date": 1768321603590,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1493,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 213,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 163,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1102,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 963,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1486,
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
          "id": "db9bce17a6cb4f3ca77e39c1311668ac1fe49473",
          "message": "Prevent client errors from going missing with async_graphql 7.1.0 (#2439)\n\n* update the result handling to prevent the errors from going missing with async_graphql 7.1.0\n\n* cargo update\n\n* update rust version\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2026-01-14T15:32:24Z",
          "tree_id": "0f985854114a4a135c1f103a1334f4e275b2cb53",
          "url": "https://github.com/Pometry/Raphtory/commit/db9bce17a6cb4f3ca77e39c1311668ac1fe49473"
        },
        "date": 1768406904619,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1465,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 192,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 137,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1109,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 995,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1521,
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
          "id": "fee245267a7660496c976776ec00513130abb787",
          "message": "Consolidate load functions, add schema for casting, and add CSV options for reading (#2423)\n\n* Added tests for loading edges from polars and from fireducks. Added a load_edges_from_polars that internally calls to_pandas() on the polars dataframe. Fireducks works.\n\n* Adding loading of data (only edges for now) from arrow directly\n\n* Adding loading of data (only edges for now) from arrow with streaming in rust instead of obtaining each column of each batch from Python individually.\n\n* Added loading of edges from DuckDB, either normally or using streaming.\n\n* Added loading edges from fireducks.pandas dataframes. General cleaning up. Committing benchmarks and tests that check graph equality when using different ingestion pathways.\n\n* Adding flag to stream/not stream data in load_* functions. Will get rid of them and always stream. Added benchmark for loading from fireducks.\n\n* Added functions for load_nodes, load_node_props, load_edges, load_edge_props, that all use the __arrow_c_stream__() interface. If a data source is passed with no __len__ function, we calculate the len ourselves. Updated ingestion benchmarks to also test pandas_streaming, fireducks_streaming, polars_streaming\n\n* Cleaned up benchmark print statements\n\n* Ran make stubs\n\n* Removed num_rows from DFView. No longer calculating/storing the total number of rows.\n\n* Cleaned up load_*_from_df functions. load_edge_props/load_node_props renamed to load_edge_metadata/load_node_metadata.\n\n* Re-added total number of rows in DFView, but as an Option. We use it if the data source provides __len__(), and if not, the loading/progress bar for loading nodes and edges doesn't show progression, only iterations per second.\n\n* Added splitting of large chunks into smaller chunks so that the progress bar for loading updates properly when using the __arrow_c_stream__ interface.\n\n* Renamed props to metadata for remaining functions\n\n* Added tests to check equality between graphs created using different ingestion pathways\n\n* Changed load_*_metadata_* back to load_*_props_*\n\n* Fixed tests and updated workflow dependencies\n\n* Added try-catch blocks for fireducks import in tests\n\n* Fixed tests and notebooks\n\n* Fixed invalid function call in test\n\n* Fixed fireducks package not available on Windows (for now anyway)\n\n* Added load_*_from_df functions to PyPersistentGraph, including load_edge_deletions_from_df\n\n* Cleaned up load_from_df tests and parametrized them to run for both event graphs/persistent graphs.\n\n* Fixed bug in tests\n\n* Removed btc dataset benchmarks\n\n* Merge cleanup and fixing python docs errors\n\n* Adding load_nodes function in python that can take any input from the following: object with __arrow_c_stream__(), parquet file/directory, csv file/directory. Added arrow-csv as a dependency to load from csv files. Added csv loading.\n\n* Fixed CSV reader to calculate column indices for each file separately.\n\n* Changed unsafe ArrowArrayStreamReader pointer cast to stream arrow data from python. Replaced it with PyRecordBatchReader::from_arrow_pycapsule for safety and future changes.\n\n* Added test for loading data from CSV\n\n* Changed CSV reading to avoid loading whole CSV files into memory in arrow format at once. Now stream 1 mil rows at a time.\n\n* Added support for mixed directories containing both CSV and parquet files.\n\n* Added schema argument to load_nodes function\n\n* Fixed load_nodes docs. Added PropType in Python. Added get_dtype_of() function on PyProperties. Added test for schema casting.\n\n* Fixed casting of columns, can use PropType variants in python to specify what type to cast columns to.\n\n* Added casting using pyarrow types as input in the schema\n\n* Added casting of nested datatypes in the data source. Added test for nested type using pyarrow Table. Cast whole RecordBatch at once now using StructArray.\n\n* Added dep:arrow-schema to \"python\" feature in raphtory-api so that DataTypes can be extracted from Python without feature gating behind arrow (larger dependency). Refactored data_type_as_prop_type to be in raphtory-api as long as any of \"arrow\", \"storage\", or \"python\" features is enabled, since they all have dep:arrow-schema.\n\n* Added support for dicts as input for the schema. Added equality comparison for PropType. Fixed previous tests and added tests for dict schema input, pyarrow types, nested (StructArray) properties, nested schemas, mixed and matched PropType and pyarrow types, both in property and in schema,...\n\n* Added CSV options for when loading CSV. Errors if CSV options were passed but no CSV files were detected.\n\n* Added schema support for Parquet and CSV files\n\n* Post merge cleanup\n\n* Added test for loading from directories (pure parquet, pure csv, mixed parquet/csv). Make sure each ingestion path returns the same node ids.\n\n* Added btc_dataset tests for loading/casting from different sources as well as failures from malformed inputs\n\n* Fixed error message displaying incorrectly when the time column is malformed (or any column). Added tests for malformed inputs in csv.\n\n* Added malformed parquet test files\n\n* Fixed CSV loader to return the same error as other loaders when a column is not found. removed extra_field parquet test bc it didn't work. cleaned up test file.\n\n* Added tests for malformed files\n\n* Added tests for compressed csv files (gz and bz2 compression).\n\n* Added test for directory with no CSV/Parquet files\n\n* Added load functions for edges, node_metadata, edge_metadata, and edge_deletions for csv files/directories\n\n* Added pyarrow.DataType import to gen-stubs.py and pyarrow-stubs to dev optional-dependencies in pyproject.toml. General clean-up before adding other functions (load_edge, load_node_metadata, ...) in python graph.\n\n* Removed load_*_from_df, load_*_from_pandas, and load_*_from_parquet functions. Added load_edges, load_node_metadata, load_edge_metadata functions to PyGraph and PyPersistentGraph. Removed Pandas loaders.\n\n* Fixed some python tests\n\n* Fixed cast_columns function to not be imported from a python feature folder which is not available in the crate root. Fixed parquet_loaders.rs.\n\n* Fixed failing tests\n\n* Added docstring for PyPropType\n\n* Fixed failing test\n\n* Changed dtype method on PyProp to return PyPropType instead of string.\n\n* Fixed tests\n\n* Fixed python doc tests\n\n* More doc fixes\n\n* Parquets need to have extension\n\n* fixed mypy failing\n\n* chore: apply tidy-public auto-fixes\n\n* Fixed CSV loader crashing on rows with missing fields. Updated missing field test on parquet and csv to ensure that nothing is loaded on None/null property values.\n\n* chore: apply tidy-public auto-fixes\n\n* Fixed docstring\n\n---------\n\nCo-authored-by: Ben Steer <b.a.steer@qmul.ac.uk>\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2026-01-16T11:32:17-05:00",
          "tree_id": "60825e2ce410034498e4671a0ec7b097795ec90a",
          "url": "https://github.com/Pometry/Raphtory/commit/fee245267a7660496c976776ec00513130abb787"
        },
        "date": 1768583309611,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 35,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 205,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 143,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1160,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 942,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1389,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "4599890+shivamka1@users.noreply.github.com",
            "name": "Shivam",
            "username": "shivamka1"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "3a2dabf26e3ac0b8b9d50dfce4ac4bce64243cda",
          "message": "impl windowing filter (#2441)\n\n* impl windowing filter\n\n* chore: apply tidy-public auto-fixes\n\n* fix viewops\n\n* solving recursive windowing\n\n* add windowing to gql\n\n* rework the dynamic dispatch for filter builders to avoid infinite recursion in window\n\n* fix warnings, ref\n\n* fix warnings, ref\n\n* ref\n\n* fix gql semantics, add tests\n\n* chore: apply tidy-public auto-fixes\n\n* fix the ordering of views in filters, add tests\n\n* rid print\n\n* ref\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>\nCo-authored-by: Lucas Jeub <lucas.jeub@pometry.com>",
          "timestamp": "2026-01-19T21:15:36Z",
          "tree_id": "43a377af7131e3bb3d5d8820d3e2edf7dfa29117",
          "url": "https://github.com/Pometry/Raphtory/commit/3a2dabf26e3ac0b8b9d50dfce4ac4bce64243cda"
        },
        "date": 1768859511419,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1475,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 214,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 150,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1208,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 1038,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1568,
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
          "id": "f494701d402d628a7a0087c5b43a08de48fd3a0f",
          "message": "Fix test issues revealed by pandas 3.0 (#2448)\n\n* remove unused import of pandas internals\n\n* attempt to fix tox tests for pandas>=3\n\n* fix test for renamed pandas dtype\n\n* rejig tests and dependency management\n\n* make dev install everything\n\n* make doc tests cleanup the output\n\n* use duckdb to read the files instead of interfacing with pandas",
          "timestamp": "2026-01-23T14:04:27+01:00",
          "tree_id": "a2c46ffa1252bafcea8e7096912074e424b58e31",
          "url": "https://github.com/Pometry/Raphtory/commit/f494701d402d628a7a0087c5b43a08de48fd3a0f"
        },
        "date": 1769175665773,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1413,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 199,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 166,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1122,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 890,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1400,
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
          "id": "8929951f7172152947da8dd83b1566351e838ac9",
          "message": "Update UI to 53ab3a3f0 (v0.2.1) (#2450)\n\n* Update UI to v0.2.1\n\n* chore: apply tidy-public auto-fixes\n\n* empty commit\n\n---------\n\nCo-authored-by: ricopinazo <38461987+ricopinazo@users.noreply.github.com>\nCo-authored-by: Pedro Rico Pinazo <ricopinazo@gmail.com>\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2026-01-23T18:05:30+01:00",
          "tree_id": "c742211bd30e62c94cd1f4abb8dc7417063662c1",
          "url": "https://github.com/Pometry/Raphtory/commit/8929951f7172152947da8dd83b1566351e838ac9"
        },
        "date": 1769190093672,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1393,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 210,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 147,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1167,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 885,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1425,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "127566471+DanielLacina@users.noreply.github.com",
            "name": "Daniel Lacina",
            "username": "DanielLacina"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "aee2d6137362052e17be686758ff35309b79fe17",
          "message": "implemented Erdos-Renyi model generation (#2253)",
          "timestamp": "2026-01-26T09:37:20+01:00",
          "tree_id": "3d2ca886c7a5e9207acc5cadea78e121c051ac1b",
          "url": "https://github.com/Pometry/Raphtory/commit/aee2d6137362052e17be686758ff35309b79fe17"
        },
        "date": 1769418814210,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1457,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 211,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 174,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1166,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 1000,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1304,
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
          "id": "5427a32f87d439134340257208afa672b30c9cb4",
          "message": "Make UI tests required again (#2455)\n\n* Make UI tests a requirement for green-ticking a PR now that they are working again.\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2026-01-27T10:35:28+01:00",
          "tree_id": "d232059b89de2e1cd363458b81dff145941ef270",
          "url": "https://github.com/Pometry/Raphtory/commit/5427a32f87d439134340257208afa672b30c9cb4"
        },
        "date": 1769508692140,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1345,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 221,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 134,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1163,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 940,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1421,
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
          "id": "e7b0141466a432b18d1069874961a90b14b90de6",
          "message": "trigger graphql cache eviction automatically (#2454)\n\n* trigger graphql cache eviction automatically\n\n* fix typo\n\n* add fixme for the evicion test\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2026-01-27T13:34:28+01:00",
          "tree_id": "098ddb44691bd91b8bd50311ed0f923c71a91e18",
          "url": "https://github.com/Pometry/Raphtory/commit/e7b0141466a432b18d1069874961a90b14b90de6"
        },
        "date": 1769519424574,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1422,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 219,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 121,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1107,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 870,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1477,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "4599890+shivamka1@users.noreply.github.com",
            "name": "Shivam",
            "username": "shivamka1"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "12be753cdc11cd84fc99bd2db90d37f03a9dbaad",
          "message": "impl in out components filter (#2458)\n\n* impl in out components filter\n\n* add review suggestions\n\n* chore: apply tidy-public auto-fixes\n\n* add review suggestions, fix docs, fix filter bug, add test\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2026-01-29T10:38:57Z",
          "tree_id": "c11751546c7645413ec2cffcbc5fded0280a5964",
          "url": "https://github.com/Pometry/Raphtory/commit/12be753cdc11cd84fc99bd2db90d37f03a9dbaad"
        },
        "date": 1769685289904,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1480,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 221,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 135,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1205,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 997,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1380,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "4599890+shivamka1@users.noreply.github.com",
            "name": "Shivam",
            "username": "shivamka1"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "5e53eee265ee607e7d4ef9fa372e502eec457d00",
          "message": "impl bool filters, add tests (#2467)\n\n* impl bool filters, add tests\n\n* fix imports\n\n* add review suggestions\n\n* add more tests\n\n* add gql tests\n\n* more tests\n\n* fix test\n\n* add more review suggestions\n\n* add more review suggestions\n\n* add delete_edge for py graph, add tests, add review suggestions\n\n* fix tests\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2026-02-04T13:35:24Z",
          "tree_id": "4e4023d527ce1171bfc7739726f73de9fe0b954a",
          "url": "https://github.com/Pometry/Raphtory/commit/5e53eee265ee607e7d4ef9fa372e502eec457d00"
        },
        "date": 1770214275579,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 34,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 208,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 142,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1126,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 909,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1336,
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
          "id": "1c299133215eb1df65ee3008b57c267769d17239",
          "message": "fixes for docker ci (#2470)",
          "timestamp": "2026-02-04T16:00:15+01:00",
          "tree_id": "d2a5290744fb917869fedf46749fccba100b44ae",
          "url": "https://github.com/Pometry/Raphtory/commit/1c299133215eb1df65ee3008b57c267769d17239"
        },
        "date": 1770219398869,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1298,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 208,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 153,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1039,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 826,
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
          "id": "233ef2cfc53e5d8aa903ed0d14ab88dcabebcdce",
          "message": "merge clis and configs (#2457)\n\n* merge clis and configs\n\n* commit missing main file\n\n* fix compilation errors and python imports\n\n* cli actually working\n\n* add server subcommand\n\n* add help messages and version option\n\n* fix compilation errors\n\n* add AppConfigBuilder import back\n\n* simplify the deadlock tests\n\n* revert ui-test commit\n\n* switch to parking_lot for deadlock tests\n\n* clear up the main.rs in raphtory-graphql\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: Fabian Murariu <2404621+fabianmurariu@users.noreply.github.com>\nCo-authored-by: Fabian Murariu <murariu.fabian@gmail.com>\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2026-02-04T18:11:48Z",
          "tree_id": "19ecc2330ef1b9a7c846d26a1ab3784954003592",
          "url": "https://github.com/Pometry/Raphtory/commit/233ef2cfc53e5d8aa903ed0d14ab88dcabebcdce"
        },
        "date": 1770230865284,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 32,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 213,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 138,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1029,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 771,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1291,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "4599890+shivamka1@users.noreply.github.com",
            "name": "Shivam",
            "username": "shivamka1"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "b4ca25faa7646c49a5e716000532a2f6ba738325",
          "message": "impl docs (#2472)\n\n* impl docs\n\n* chore: apply tidy-public auto-fixes\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2026-02-06T23:22:46Z",
          "tree_id": "2d319305e19d4e5ef1a11551ddb1e1ac9c79f98a",
          "url": "https://github.com/Pometry/Raphtory/commit/b4ca25faa7646c49a5e716000532a2f6ba738325"
        },
        "date": 1770422318685,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1357,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 210,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 139,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1111,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 953,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1178,
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
          "id": "470706294e8a740d9127f34cf46d95c93ad38bb1",
          "message": "fix the schema casting (#2479)",
          "timestamp": "2026-02-09T14:40:49+01:00",
          "tree_id": "151e9384aff3f9e45337594d4c5deef3c8f198d7",
          "url": "https://github.com/Pometry/Raphtory/commit/470706294e8a740d9127f34cf46d95c93ad38bb1"
        },
        "date": 1770646703829,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 1302,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 195,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 139,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1120,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 826,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1366,
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
          "id": "e37b551c9d2edefdc79a6412719b8b50dba6be88",
          "message": "embedding api improvements (#2249)\n\n* embedding api improvements\n\n* disable embeddings by default\n\n* fix compilation error for enable_embeddings\n\n* fix compilation error in main.rs\n\n* sort milvus integration\n\n* lancedb vector storage implementation and rename score to distance\n\n* like this almost compiles but not yet\n\n* force lancedb to work with other version of chrono but still polars-arrow complaining\n\n* this seems to compile with all features but storage\n\n* put pometry-storage back into place\n\n* sort new multi-embedding vector cache\n\n* still some rust tests failing\n\n* remove outdated comment\n\n* all rust tests now passing with the new custom openai server\n\n* some compilation errors caused by teh server future not being Sync\n\n* fixing some python tests\n\n* wip\n\n* trying to avoid the drop of the tempdir but still not working\n\n* fix compilation error\n\n* fix bug caused by a temp dir being dropped too soon\n\n* fix python tests\n\n* fix dependency conflicts\n\n* format\n\n* fix rust test\n\n* change rust version\n\n* started implementing context manager for PyEmbeddingServer\n\n* context manager for embedding server\n\n* all graphql vector tests are passing now\n\n* re-indexing, graphql vectorise, and minor fixes\n\n* big cleanup\n\n* update Cargo.lock\n\n* handle all unwraps in lancedb.rs\n\n* fix cache tests\n\n* fix benchmark compilation error\n\n* fix python graphql vector tests\n\n* fix more graphql tests\n\n* this should fix all the compilation errors on the tests\n\n* make vector cache lazy\n\n* fix rust doc tests\n\n* fix compilation error on a test and expose reindex api\n\n* handle python embedding errors\n\n* try a different way of making the vector cache lazy\n\n* rely only on id and actual vector, not distance, for lancedb index test\n\n* fix mutable graph test\n\n* remove unused comments\n\n* try different ports for mutable_graph tests\n\n* fix test_server_start_with_failing_embedding test\n\n* remove unused imports\n\n* disable all features for lancedb\n\n* tune cargo in rust test action\n\n* try to fix cargo in the python test action as well\n\n* fix python tests\n\n* disable cargo tuning for python tests\n\n* try to avoid the no space left on devide when running the graphql bench\n\n* add df -h in all steps\n\n* try removing target a bit earlier\n\n* replace one-off vectors with random smaller ones\n\n* apply same fix no space left on device for stress test\n\n* chore: apply tidy-public auto-fixes\n\n* chore: apply tidy-public auto-fixes\n\n* remove outdated comments\n\n* change custom embedding server api\n\n* remove outdated comment\n\n* change Embedding to be Float32Array removes some needless iter and copy\n\n* implement vectorise_all_graphs in python\n\n* fix insertion bug cause by missing append call\n\n* change imports to satisfy cargo check\n\n* address some of the comments\n\n* remove one unwrap out of closure in lancedb.rs\n\n* replace collect appropriately\n\n* wrap raphtory access with blocking_compute\n\n* revert back ci changes\n\n* remove unused prints on python tests\n\n* put back CARGO_BUILD_JOBS: 2\n\n* chore: apply tidy-public auto-fixes\n\n* changes to caching\n\n* fixes for tokio\n\n* fixes for stable embedding cache\n\n* remove printlns\n\n* remove useless print and extract vector cache\n\n* fix the python tests\n\n* more python tests\n\n* chore: apply tidy-public auto-fixes\n\n* minor changes before merge\n\n---------\n\nCo-authored-by: github-actions[bot] <github-actions[bot]@users.noreply.github.com>\nCo-authored-by: Fabian Murariu <murariu.fabian@gmail.com>\nCo-authored-by: Ben Steer <b.a.steer@qmul.ac.uk>",
          "timestamp": "2026-02-13T17:03:01Z",
          "tree_id": "516dd5c5e144555b73585b6a181fd27f4bb9324d",
          "url": "https://github.com/Pometry/Raphtory/commit/e37b551c9d2edefdc79a6412719b8b50dba6be88"
        },
        "date": 1771004601333,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 0,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 217,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 157,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1083,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 854,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1312,
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
          "id": "92e9cbd84263a19e6e201145034b8b95323586c2",
          "message": "fix auto release action (#2498)",
          "timestamp": "2026-02-18T17:21:47+01:00",
          "tree_id": "2ae2b15f81025e80219519953c29cf159b5d7041",
          "url": "https://github.com/Pometry/Raphtory/commit/92e9cbd84263a19e6e201145034b8b95323586c2"
        },
        "date": 1771434179064,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "addNode",
            "value": 24,
            "unit": "req/s"
          },
          {
            "name": "randomNodePage",
            "value": 201,
            "unit": "req/s"
          },
          {
            "name": "randomEdgePage",
            "value": 112,
            "unit": "req/s"
          },
          {
            "name": "nodePropsByName",
            "value": 1055,
            "unit": "req/s"
          },
          {
            "name": "nodeNeighboursByName",
            "value": 857,
            "unit": "req/s"
          },
          {
            "name": "readAndWriteNodeProperties",
            "value": 1354,
            "unit": "req/s"
          }
        ]
      }
    ]
  }
}