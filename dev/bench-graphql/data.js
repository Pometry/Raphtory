window.BENCHMARK_DATA = {
  "lastUpdate": 1758028480550,
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
      }
    ]
  }
}