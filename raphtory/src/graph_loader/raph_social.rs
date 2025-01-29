use crate::{
    core::Prop,
    io::csv_loader::CsvLoader,
    prelude::{AdditionOps, Graph, NO_PROPS},
};
use chrono::DateTime;
use raphtory_api::core::storage::arc_str::ArcStr;
use std::{fs, path::PathBuf, time::Instant};

use crate::prelude::{GraphViewOps, NodeViewOps, StableEncode};
use csv::Writer;
use fake::{
    faker::{
        internet::en::IP,
        lorem::en::Sentence,
        name::en::{FirstName, LastName},
    },
    Fake,
};
use itertools::Itertools;
use rand::{prelude::SliceRandom, thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::{error::Error, io::Write};

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

pub fn generate_fake_data(
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
    let mut person_forum_writer =
        Writer::from_path(format!("{}/person_forum_relationships.csv", output_dir))?;
    let mut posts_writer = Writer::from_path(format!("{}/posts.csv", output_dir))?;
    let mut post_forum_writer =
        Writer::from_path(format!("{}/post_forum_relationships.csv", output_dir))?;
    let mut comments_writer = Writer::from_path(format!("{}/comments.csv", output_dir))?;
    let mut comment_post_writer =
        Writer::from_path(format!("{}/comment_post_relationships.csv", output_dir))?;

    fn gen_timestamp(rng: &mut impl Rng) -> i64 {
        rng.gen_range(946684800000..1609459200000) // Random timestamp from 2000 to 2020
    }

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
            person_forum_writer.serialize(
                PersonForum {
                    person_id: format!("person_{}", i),
                    forum_id: format!("forum_{}", rng.gen_range(1..=num_forums)),
                    is_moderator: rng.gen_bool(0.1),
                    join_date: gen_timestamp(&mut rng),
                    activity_score: rng.gen_range(0.0..100.0),
                },
            )?;
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
        comment_post_writer.serialize(
            CommentPost {
                comment_id: format!("comment_{}", i),
                post_id: format!("post_{}", rng.gen_range(1..=num_posts)),
                creation_date, // Use comment's creation date
                is_edited: rng.gen_bool(0.1),
                upvotes: rng.gen_range(0..200),
                reply_count: rng.gen_range(0..20),
            },
        )?;
    }
    comments_writer.flush()?;
    comment_post_writer.flush()?;

    Ok(())
}

pub fn load_and_save_raph_social_graph(output_dir: &str) -> Graph {
    let people_csv_path = PathBuf::from(output_dir).join("people.csv");
    let forums_csv_path = PathBuf::from(output_dir).join("forums.csv");
    let posts_csv_path = PathBuf::from(output_dir).join("posts.csv");
    let comments_csv_path = PathBuf::from(output_dir).join("comments.csv");
    let person_forum_csv_path = PathBuf::from(output_dir).join("person_forum_relationships.csv");
    let post_forum_csv_path = PathBuf::from(output_dir).join("post_forum_relationships.csv");
    let comment_post_csv_path = PathBuf::from(output_dir).join("comment_post_relationships.csv");

    let g = Graph::new();
    let now = Instant::now();

    CsvLoader::new(people_csv_path.clone())
        .set_header(true)
        .set_delimiter(",")
        .load_into_graph(&g, |person: Person, g: &Graph| {
            let id = person.id;
            let first_name = person.first_name;
            let last_name = person.last_name;
            let gender = person.gender;
            let birthday = person.birthday;
            let creation_date = person.creation_date;

            g.add_node(
                DateTime::from_timestamp(creation_date, 0).unwrap(),
                id.clone(),
                NO_PROPS,
                Some("person"),
            )
            .expect("Failed to add node")
            .add_constant_properties([
                ("first_name", Prop::Str(ArcStr::from(first_name))),
                ("last_name", Prop::Str(ArcStr::from(last_name))),
                ("gender", Prop::Str(ArcStr::from(gender))),
                ("birthday", Prop::I64(birthday)),
            ])
            .expect("Failed to add node static property");
        })
        .expect("Failed to load graph from CSV data files");

    println!(
        "Loaded graph from CSV data files {} with {} nodes, {} edges which took {} seconds",
        people_csv_path.clone().as_path().to_str().unwrap(),
        g.count_nodes(),
        g.count_edges(),
        now.elapsed().as_secs()
    );

    CsvLoader::new(forums_csv_path.clone())
        .set_header(true)
        .set_delimiter(",")
        .load_into_graph(&g, |forum: Forum, g: &Graph| {
            let id = forum.id;
            let title = forum.title;
            let creation_date = forum.creation_date;

            g.add_node(
                DateTime::from_timestamp(creation_date, 0).unwrap(),
                id.clone(),
                NO_PROPS,
                Some("forum"),
            )
            .expect("Failed to add node")
            .add_constant_properties([("title", Prop::Str(ArcStr::from(title)))])
            .expect("Failed to add node static property");
        })
        .expect("Failed to load graph from CSV data files");

    println!(
        "Loaded graph from CSV data files {} with {} nodes, {} edges which took {} seconds",
        forums_csv_path.clone().as_path().to_str().unwrap(),
        g.count_nodes(),
        g.count_edges(),
        now.elapsed().as_secs()
    );

    CsvLoader::new(posts_csv_path.clone())
        .set_header(true)
        .set_delimiter(",")
        .load_into_graph(&g, |post: Post, g: &Graph| {
            let id = post.id;
            let creator_id = post.creator_id;
            let creation_date = post.creation_date;
            let location_ip = post.location_ip;
            let browser_used = post.browser_used;
            let content = post.content;
            let length = post.length;

            g.add_node(
                DateTime::from_timestamp(creation_date, 0).unwrap(),
                id.clone(),
                [
                    ("content", Prop::Str(ArcStr::from(content))),
                    ("length", Prop::U64(length)),
                    ("location_ip", Prop::Str(ArcStr::from(location_ip))),
                    ("browser_used", Prop::Str(ArcStr::from(browser_used))),
                ],
                Some("post"),
            )
            .expect("Failed to add node")
            .add_constant_properties([("creator_id", Prop::Str(ArcStr::from(creator_id)))])
            .expect("Failed to add node static property");
        })
        .expect("Failed to load graph from CSV data files");

    println!(
        "Loaded graph from CSV data files {} with {} nodes, {} edges which took {} seconds",
        posts_csv_path.clone().as_path().to_str().unwrap(),
        g.count_nodes(),
        g.count_edges(),
        now.elapsed().as_secs()
    );

    CsvLoader::new(comments_csv_path.clone())
        .set_header(true)
        .set_delimiter(",")
        .load_into_graph(&g, |comment: Comment, g: &Graph| {
            let id = comment.id;
            let creator_id = comment.creator_id;
            let creation_date = comment.creation_date;
            let location_ip = comment.location_ip;
            let browser_used = comment.browser_used;
            let content = comment.content;
            let length = comment.length;

            g.add_node(
                DateTime::from_timestamp(creation_date, 0).unwrap(),
                id.clone(),
                [
                    ("content", Prop::Str(ArcStr::from(content))),
                    ("length", Prop::U64(length)),
                    ("location_ip", Prop::Str(ArcStr::from(location_ip))),
                    ("browser_used", Prop::Str(ArcStr::from(browser_used))),
                ],
                Some("comment"),
            )
            .expect("Failed to add node")
            .add_constant_properties([("creator_id", Prop::Str(ArcStr::from(creator_id)))])
            .expect("Failed to add node static property");
        })
        .expect("Failed to load graph from CSV data files");

    println!(
        "Loaded graph from CSV data files {} with {} nodes, {} edges which took {} seconds",
        comments_csv_path.clone().as_path().to_str().unwrap(),
        g.count_nodes(),
        g.count_edges(),
        now.elapsed().as_secs()
    );

    CsvLoader::new(person_forum_csv_path.clone())
        .set_header(true)
        .set_delimiter(",")
        .load_into_graph(&g, |person_forum: PersonForum, g: &Graph| {
            let person_id = person_forum.person_id;
            let forum_id = person_forum.forum_id;
            let is_moderator = person_forum.is_moderator;
            let join_date = person_forum.join_date;
            let activity_score = person_forum.activity_score;

            g.add_edge(
                DateTime::from_timestamp(join_date, 0).unwrap(),
                person_id.clone(),
                forum_id.clone(),
                [
                    ("activity_score", Prop::F64(activity_score)),
                    ("is_moderator", Prop::Bool(is_moderator)),
                ],
                None,
            )
            .expect("Failed to add node")
            .add_constant_properties(NO_PROPS, None)
            .expect("Failed to add node static property");
        })
        .expect("Failed to load graph from CSV data files");

    println!(
        "Loaded graph from CSV data files {} with {} nodes, {} edges which took {} seconds",
        person_forum_csv_path.clone().as_path().to_str().unwrap(),
        g.count_nodes(),
        g.count_edges(),
        now.elapsed().as_secs()
    );

    CsvLoader::new(post_forum_csv_path.clone())
        .set_header(true)
        .set_delimiter(",")
        .load_into_graph(&g, |post_forum: PostForum, g: &Graph| {
            let post_id = post_forum.post_id;
            let forum_id = post_forum.forum_id;
            let creation_date = post_forum.creation_date;
            let is_featured = post_forum.is_featured;
            let likes_count = post_forum.likes_count;
            let comments_count = post_forum.comments_count;

            g.add_edge(
                DateTime::from_timestamp(creation_date, 0).unwrap(),
                post_id.clone(),
                forum_id.clone(),
                [
                    ("is_featured", Prop::Bool(is_featured)),
                    ("likes_count", Prop::U64(likes_count)),
                    ("comments_count", Prop::U64(comments_count)),
                ],
                None,
            )
            .expect("Failed to add node")
            .add_constant_properties(NO_PROPS, None)
            .expect("Failed to add node static property");
        })
        .expect("Failed to load graph from CSV data files");

    println!(
        "Loaded graph from CSV data files {} with {} nodes, {} edges which took {} seconds",
        person_forum_csv_path.clone().as_path().to_str().unwrap(),
        g.count_nodes(),
        g.count_edges(),
        now.elapsed().as_secs()
    );

    CsvLoader::new(comment_post_csv_path.clone())
        .set_header(true)
        .set_delimiter(",")
        .load_into_graph(&g, |comment_post: CommentPost, g: &Graph| {
            let comment_id = comment_post.comment_id;
            let post_id = comment_post.post_id;
            let creation_date = comment_post.creation_date;
            let is_edited = comment_post.is_edited;
            let upvotes = comment_post.upvotes;
            let reply_count = comment_post.reply_count;

            g.add_edge(
                DateTime::from_timestamp(creation_date, 0).unwrap(),
                comment_id.clone(),
                post_id.clone(),
                [
                    ("is_edited", Prop::Bool(is_edited)),
                    ("upvotes", Prop::U64(upvotes)),
                    ("reply_count", Prop::U64(reply_count)),
                ],
                None,
            )
            .expect("Failed to add node")
            .add_constant_properties(NO_PROPS, None)
            .expect("Failed to add node static property");
        })
        .expect("Failed to load graph from CSV data files");

    println!(
        "Loaded graph from CSV data files {} with {} nodes, {} edges which took {} seconds",
        person_forum_csv_path.clone().as_path().to_str().unwrap(),
        g.count_nodes(),
        g.count_edges(),
        now.elapsed().as_secs()
    );

    let data_dir = "/tmp/graphs/raph_social/rf0.1";
    g.encode(data_dir).expect("Failed to save graph");

    g
}

#[cfg(test)]
mod test_raph_social {
    use crate::{graph_loader::raph_social::load_and_save_raph_social_graph, prelude::Graph};
    use crate::graph_loader::raph_social::generate_fake_data;

    #[test]
    fn test_generate_fake() {
        generate_fake_data("output", 3000, 500, 70000, 100_000).unwrap();
    }

    #[test]
    fn test_raph_social_graph_load() {
        let output_dir = "output";
        load_and_save_raph_social_graph(output_dir);
    }
}
