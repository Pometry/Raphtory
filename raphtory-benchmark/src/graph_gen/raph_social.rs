use chrono::DateTime;
use raphtory::{
    core::Prop,
    io::csv_loader::CsvLoader,
    prelude::{AdditionOps, Graph, NO_PROPS},
};
use raphtory_api::core::storage::arc_str::ArcStr;
use std::{fs, path::PathBuf, time::Instant};

use csv::Writer;
use fake::{
    faker::{
        internet::en::IP,
        lorem::en::Sentence,
        name::en::{FirstName, LastName},
    },
    Fake,
};
use rand::{prelude::SliceRandom, thread_rng, Rng};
use raphtory::prelude::*;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{collections::HashMap, error::Error, fmt::Debug};

#[derive(Debug, Deserialize, Serialize)]
pub struct Person {
    pub creation_date: i64,
    pub id: String,
    pub first_name: String,
    pub last_name: String,
    pub gender: String,
    pub birthday: i64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PersonForum {
    pub join_date: i64,
    pub person_id: String,
    pub forum_id: String,
    pub is_moderator: bool,
    pub activity_score: f64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Forum {
    pub creation_date: i64,
    pub id: String,
    pub title: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Post {
    pub creation_date: i64,
    pub id: String,
    pub creator_id: String,
    pub location_ip: String,
    pub browser_used: String,
    pub content: String,
    pub length: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PostForum {
    pub creation_date: i64,
    pub post_id: String,
    pub forum_id: String,
    pub is_featured: bool,
    pub likes_count: u64,
    pub comments_count: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Comment {
    pub creation_date: i64,
    pub id: String,
    pub creator_id: String,
    pub location_ip: String,
    pub browser_used: String,
    pub content: String,
    pub length: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CommentPost {
    pub creation_date: i64,
    pub comment_id: String,
    pub post_id: String,
    pub is_edited: bool,
    pub upvotes: u64,
    pub reply_count: u64,
}

fn gen_timestamp(rng: &mut impl Rng) -> i64 {
    rng.gen_range(946684800000..1609459200000) // Random timestamp from 2000 to 2020
}

pub fn generate_data_write_to_csv(
    output_dir: &str,
    num_people: usize,
    num_forums: usize,
    num_posts: usize,
    num_comments: usize,
) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(output_dir)?;

    let mut rng = thread_rng();

    // Create writers for each file
    let mut people_writer = Writer::from_path(format!("{}/people.csv", output_dir))?;
    let mut forums_writer = Writer::from_path(format!("{}/forums.csv", output_dir))?;
    let mut person_forum_writer = Writer::from_path(format!("{}/person_forum.csv", output_dir))?;
    let mut posts_writer = Writer::from_path(format!("{}/posts.csv", output_dir))?;
    let mut post_forum_writer = Writer::from_path(format!("{}/post_forum.csv", output_dir))?;
    let mut comments_writer = Writer::from_path(format!("{}/comments.csv", output_dir))?;
    let mut comment_post_writer = Writer::from_path(format!("{}/comment_post.csv", output_dir))?;

    // People iterator
    for i in 1..=num_people {
        people_writer.serialize(Person {
            id: format!("person_{}", i),
            first_name: FirstName().fake(),
            last_name: LastName().fake(),
            gender: if rng.gen_bool(0.5) {
                "male".to_string()
            } else {
                "female".to_string()
            },
            birthday: gen_timestamp(&mut rng),
            creation_date: gen_timestamp(&mut rng),
        })?;
    }
    people_writer.flush()?;

    // Forums iterator
    for i in 1..=num_forums {
        forums_writer.serialize(Forum {
            id: format!("forum_{}", i),
            title: Sentence(1..3).fake(),
            creation_date: gen_timestamp(&mut rng),
        })?;
    }
    forums_writer.flush()?;

    // Person-Forum Relationships
    for i in 1..=num_people {
        let membership_count = rng.gen_range(1..=3);
        for _ in 0..membership_count {
            person_forum_writer.serialize(PersonForum {
                person_id: format!("person_{}", i),
                forum_id: format!("forum_{}", rng.gen_range(1..=num_forums)),
                is_moderator: rng.gen_bool(0.1),
                join_date: gen_timestamp(&mut rng),
                activity_score: rng.gen_range(0.0..100.0),
            })?;
        }
    }
    person_forum_writer.flush()?;

    // Posts iterator
    for i in 1..=num_posts {
        let creation_date = gen_timestamp(&mut rng);
        posts_writer.serialize(Post {
            id: format!("post_{}", i),
            creator_id: format!("person_{}", rng.gen_range(1..=num_people)),
            creation_date,
            location_ip: IP().fake(),
            browser_used: ["Chrome", "Firefox", "Safari", "Edge"]
                .choose(&mut rng)
                .unwrap()
                .to_string(),
            content: Sentence(5..15).fake(),
            length: rng.gen_range(20..200),
        })?;
        post_forum_writer.serialize(PostForum {
            post_id: format!("post_{}", i),
            forum_id: format!("forum_{}", rng.gen_range(1..=num_forums)),
            creation_date, // Use post's creation date
            is_featured: rng.gen_bool(0.2),
            likes_count: rng.gen_range(0..500),
            comments_count: rng.gen_range(0..200),
        })?;
    }
    posts_writer.flush()?;
    post_forum_writer.flush()?;

    // Comments iterator
    for i in 1..=num_comments {
        let creation_date = gen_timestamp(&mut rng);
        comments_writer.serialize(Comment {
            id: format!("comment_{}", i),
            creator_id: format!("person_{}", rng.gen_range(1..=num_people)),
            creation_date,
            location_ip: IP().fake(),
            browser_used: ["Chrome", "Firefox", "Safari", "Edge"]
                .choose(&mut rng)
                .unwrap()
                .to_string(),
            content: Sentence(5..15).fake(),
            length: rng.gen_range(50..500),
        })?;
        comment_post_writer.serialize(CommentPost {
            comment_id: format!("comment_{}", i),
            post_id: format!("post_{}", rng.gen_range(1..=num_posts)),
            creation_date, // Use comment's creation date
            is_edited: rng.gen_bool(0.1),
            upvotes: rng.gen_range(0..200),
            reply_count: rng.gen_range(0..20),
        })?;
    }
    comments_writer.flush()?;
    comment_post_writer.flush()?;

    Ok(())
}

fn load_nodes<T, F>(graph: &Graph, path: &PathBuf, load_fn: F)
where
    T: DeserializeOwned + Debug + Send + Sync,
    F: Fn(T, &Graph) + Send + Sync,
{
    let now = Instant::now();
    CsvLoader::new(path.clone())
        .set_header(true)
        .set_delimiter(",")
        .load_into_graph(graph, load_fn)
        .expect("Failed to load graph from CSV");

    println!(
        "Loaded nodes from {} with {} nodes, {} edges in {} seconds",
        path.to_str().unwrap(),
        graph.count_nodes(),
        graph.count_edges(),
        now.elapsed().as_secs()
    );
}

fn load_edges<T, F>(graph: &Graph, path: &PathBuf, load_fn: F)
where
    T: DeserializeOwned + Debug + Send + Sync,
    F: Fn(T, &Graph) + Send + Sync,
{
    let now = Instant::now();
    CsvLoader::new(path.clone())
        .set_header(true)
        .set_delimiter(",")
        .load_into_graph(graph, load_fn)
        .expect("Failed to load graph from CSV");

    println!(
        "Loaded edges from {} with {} nodes, {} edges in {} seconds",
        path.to_str().unwrap(),
        graph.count_nodes(),
        graph.count_edges(),
        now.elapsed().as_secs()
    );
}

pub fn load_graph(data_dir: &str) -> Result<Graph, Box<dyn Error>> {
    let csv_paths = [
        ("people", "people.csv"),
        ("forums", "forums.csv"),
        ("posts", "posts.csv"),
        ("comments", "comments.csv"),
        ("person_forum", "person_forum.csv"),
        ("post_forum", "post_forum.csv"),
        ("comment_post", "comment_post.csv"),
    ]
    .iter()
    .map(|(key, file)| (*key, PathBuf::from(data_dir).join(file)))
    .collect::<HashMap<&str, PathBuf>>();

    let g = Graph::new();

    // Load nodes
    load_nodes(&g, &csv_paths["people"], |person: Person, g| {
        g.add_node(
            DateTime::from_timestamp(person.creation_date, 0).unwrap(),
            person.id.clone(),
            NO_PROPS,
            Some("person"),
        )
        .expect("Failed to add node")
        .add_constant_properties([
            ("first_name", Prop::Str(ArcStr::from(person.first_name))),
            ("last_name", Prop::Str(ArcStr::from(person.last_name))),
            ("gender", Prop::Str(ArcStr::from(person.gender))),
            ("birthday", Prop::I64(person.birthday)),
        ])
        .expect("Failed to add node static property");
    });

    load_nodes(&g, &csv_paths["forums"], |forum: Forum, g| {
        g.add_node(
            DateTime::from_timestamp(forum.creation_date, 0).unwrap(),
            forum.id.clone(),
            NO_PROPS,
            Some("forum"),
        )
        .expect("Failed to add node")
        .add_constant_properties([("title", Prop::Str(ArcStr::from(forum.title)))])
        .expect("Failed to add node static property");
    });

    load_nodes(&g, &csv_paths["posts"], |post: Post, g| {
        g.add_node(
            DateTime::from_timestamp(post.creation_date, 0).unwrap(),
            post.id.clone(),
            [
                ("content", Prop::Str(ArcStr::from(post.content))),
                ("length", Prop::U64(post.length)),
                ("location_ip", Prop::Str(ArcStr::from(post.location_ip))),
                ("browser_used", Prop::Str(ArcStr::from(post.browser_used))),
            ],
            Some("post"),
        )
        .expect("Failed to add node")
        .add_constant_properties([("creator_id", Prop::Str(ArcStr::from(post.creator_id)))])
        .expect("Failed to add node static property");
    });

    load_nodes(&g, &csv_paths["comments"], |comment: Comment, g| {
        g.add_node(
            DateTime::from_timestamp(comment.creation_date, 0).unwrap(),
            comment.id.clone(),
            [
                ("content", Prop::Str(ArcStr::from(comment.content))),
                ("length", Prop::U64(comment.length)),
                ("location_ip", Prop::Str(ArcStr::from(comment.location_ip))),
                (
                    "browser_used",
                    Prop::Str(ArcStr::from(comment.browser_used)),
                ),
            ],
            Some("comment"),
        )
        .expect("Failed to add node")
        .add_constant_properties([("creator_id", Prop::Str(ArcStr::from(comment.creator_id)))])
        .expect("Failed to add node static property");
    });

    // Load edges
    load_edges(&g, &csv_paths["person_forum"], |rel: PersonForum, g| {
        g.add_edge(
            DateTime::from_timestamp(rel.join_date, 0).unwrap(),
            rel.person_id.clone(),
            rel.forum_id.clone(),
            [
                ("activity_score", Prop::F64(rel.activity_score)),
                ("is_moderator", Prop::Bool(rel.is_moderator)),
            ],
            None,
        )
        .expect("Failed to add edge");
    });

    load_edges(&g, &csv_paths["post_forum"], |rel: PostForum, g| {
        g.add_edge(
            DateTime::from_timestamp(rel.creation_date, 0).unwrap(),
            rel.post_id.clone(),
            rel.forum_id.clone(),
            [
                ("is_featured", Prop::Bool(rel.is_featured)),
                ("likes_count", Prop::U64(rel.likes_count)),
                ("comments_count", Prop::U64(rel.comments_count)),
            ],
            None,
        )
        .expect("Failed to add edge");
    });

    load_edges(&g, &csv_paths["comment_post"], |rel: CommentPost, g| {
        g.add_edge(
            DateTime::from_timestamp(rel.creation_date, 0).unwrap(),
            rel.comment_id.clone(),
            rel.post_id.clone(),
            [
                ("is_edited", Prop::Bool(rel.is_edited)),
                ("upvotes", Prop::U64(rel.upvotes)),
                ("reply_count", Prop::U64(rel.reply_count)),
            ],
            None,
        )
        .expect("Failed to add edge");
    });
    Ok(g)
}

pub fn load_graph_save(data_dir: &str, output_dir: &str) -> Result<Graph, Box<dyn Error>> {
    fs::create_dir_all(output_dir)?;
    let g = load_graph(data_dir)?;
    g.encode(output_dir).expect("Failed to save graph");
    Ok(g)
}

pub fn generate_data_load_graph_save(
    output_dir: &str,
    num_people: usize,
    num_forums: usize,
    num_posts: usize,
    num_comments: usize,
) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(output_dir)?;

    let mut rng = thread_rng();
    let graph = Graph::new();

    // People
    for i in 1..=num_people {
        let person_id = format!("person_{}", i);
        let creation_date = gen_timestamp(&mut rng);

        graph
            .add_node(
                DateTime::from_timestamp(creation_date, 0).unwrap(),
                person_id.clone(),
                NO_PROPS,
                Some("person"),
            )
            .expect("Failed to add person node")
            .add_constant_properties([
                (
                    "first_name",
                    Prop::Str(ArcStr::from(FirstName().fake::<String>())),
                ),
                (
                    "last_name",
                    Prop::Str(ArcStr::from(LastName().fake::<String>())),
                ),
                (
                    "gender",
                    Prop::Str(ArcStr::from(if rng.gen_bool(0.5) {
                        "male"
                    } else {
                        "female"
                    })),
                ),
                ("birthday", Prop::I64(gen_timestamp(&mut rng))),
            ])
            .expect("Failed to add person properties");
    }

    // Forums
    for i in 1..=num_forums {
        let forum_id = format!("forum_{}", i);
        let creation_date = gen_timestamp(&mut rng);

        graph
            .add_node(
                DateTime::from_timestamp(creation_date, 0).unwrap(),
                forum_id.clone(),
                NO_PROPS,
                Some("forum"),
            )
            .expect("Failed to add forum node")
            .add_constant_properties([(
                "title",
                Prop::Str(ArcStr::from(Sentence(1..3).fake::<String>())),
            )])
            .expect("Failed to add forum properties");
    }

    // Person Forum
    for i in 1..=num_people {
        let person_id = format!("person_{}", i);
        let membership_count = rng.gen_range(1..=3);
        for _ in 0..membership_count {
            let forum_id = format!("forum_{}", rng.gen_range(1..=num_forums));
            graph
                .add_edge(
                    DateTime::from_timestamp(gen_timestamp(&mut rng), 0).unwrap(),
                    person_id.clone(),
                    forum_id.clone(),
                    [
                        ("activity_score", Prop::F64(rng.gen_range(0.0..100.0))),
                        ("is_moderator", Prop::Bool(rng.gen_bool(0.1))),
                    ],
                    None,
                )
                .expect("Failed to add person-forum edge");
        }
    }

    // Posts, Post Forum
    for i in 1..=num_posts {
        let post_id = format!("post_{}", i);
        let creator_id = format!("person_{}", rng.gen_range(1..=num_people));
        let creation_date = gen_timestamp(&mut rng);

        graph
            .add_node(
                DateTime::from_timestamp(creation_date, 0).unwrap(),
                post_id.clone(),
                [
                    (
                        "content",
                        Prop::Str(ArcStr::from(Sentence(5..15).fake::<String>())),
                    ),
                    ("length", Prop::U64(rng.gen_range(20..200))),
                    (
                        "location_ip",
                        Prop::Str(ArcStr::from(IP().fake::<String>())),
                    ),
                    (
                        "browser_used",
                        Prop::Str(ArcStr::from(
                            ["Chrome", "Firefox", "Safari", "Edge"]
                                .choose(&mut rng)
                                .unwrap()
                                .to_string(),
                        )),
                    ),
                ],
                Some("post"),
            )
            .expect("Failed to add post node")
            .add_constant_properties([("creator_id", Prop::Str(ArcStr::from(creator_id.clone())))])
            .expect("Failed to add post properties");

        let forum_id = format!("forum_{}", rng.gen_range(1..=num_forums));
        graph
            .add_edge(
                DateTime::from_timestamp(creation_date, 0).unwrap(),
                post_id.clone(),
                forum_id.clone(),
                [
                    ("is_featured", Prop::Bool(rng.gen_bool(0.2))),
                    ("likes_count", Prop::U64(rng.gen_range(0..500))),
                    ("comments_count", Prop::U64(rng.gen_range(0..200))),
                ],
                None,
            )
            .expect("Failed to add post-forum edge");
    }

    // Comments, Comment Forum
    for i in 1..=num_comments {
        let comment_id = format!("comment_{}", i);
        let creator_id = format!("person_{}", rng.gen_range(1..=num_people));
        let creation_date = gen_timestamp(&mut rng);

        graph
            .add_node(
                DateTime::from_timestamp(creation_date, 0).unwrap(),
                comment_id.clone(),
                [
                    (
                        "content",
                        Prop::Str(ArcStr::from(Sentence(5..15).fake::<String>())),
                    ),
                    ("length", Prop::U64(rng.gen_range(50..500))),
                    (
                        "location_ip",
                        Prop::Str(ArcStr::from(IP().fake::<String>())),
                    ),
                    (
                        "browser_used",
                        Prop::Str(ArcStr::from(
                            ["Chrome", "Firefox", "Safari", "Edge"]
                                .choose(&mut rng)
                                .unwrap()
                                .to_string(),
                        )),
                    ),
                ],
                Some("comment"),
            )
            .expect("Failed to add comment node")
            .add_constant_properties([("creator_id", Prop::Str(ArcStr::from(creator_id.clone())))])
            .expect("Failed to add comment properties");

        let post_id = format!("post_{}", rng.gen_range(1..=num_posts));
        graph
            .add_edge(
                DateTime::from_timestamp(creation_date, 0).unwrap(),
                comment_id.clone(),
                post_id.clone(),
                [
                    ("is_edited", Prop::Bool(rng.gen_bool(0.1))),
                    ("upvotes", Prop::U64(rng.gen_range(0..200))),
                    ("reply_count", Prop::U64(rng.gen_range(0..20))),
                ],
                None,
            )
            .expect("Failed to add comment-post edge");
    }

    graph.encode(output_dir).expect("Failed to save graph");

    Ok(())
}

#[cfg(test)]
mod test_raph_social {
    use crate::graph_gen::raph_social::{
        generate_data_load_graph_save, generate_data_write_to_csv, load_graph, load_graph_save,
    };
    use raphtory::{
        db::graph::graph::assert_graph_equal,
        prelude::{Graph, StableDecode},
    };

    #[test]
    fn test_generate_data_write_to_csv() {
        generate_data_write_to_csv("output", 3000, 500, 70000, 100_000).unwrap();
    }

    #[test]
    #[ignore]
    fn generate_small_test_data() {
        generate_data_write_to_csv("test_data/csv", 10, 10, 10, 10).unwrap();
    }

    #[test]
    #[ignore]
    fn save_small_test_protobuf() {
        load_graph_save("test_data/csv", "test_data/graphs/test_graph_0_15").unwrap();
    }

    #[test]
    #[ignore] // TODO: Fix this
    fn test_load_graph_save() {
        let data_dir = "output";
        let output_dir = "/tmp/graphs/raph_social/rf0.1";
        load_graph_save(data_dir, output_dir).unwrap();
    }

    #[test]
    #[ignore] // TODO: Fix this
    fn test_generate_data_load_graph_save() {
        generate_data_load_graph_save("/tmp/graphs/raph_social/rf0.1", 3000, 500, 70000, 100_000)
            .unwrap();
    }

    #[test]
    fn test_proto_decode_0_15() {
        let g_expected = load_graph("test_data/csv").unwrap();
        let g_from_proto = Graph::decode("test_data/graphs/test_graph_0_15").unwrap();
        assert_graph_equal(&g_from_proto, &g_expected);
    }
}
