pub mod ast;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "parser/cypher.pest"]
pub struct CypherParser;

#[cfg(test)]
mod test {

    use super::*;
    use pest::Parser;

    #[test]
    fn match_1() {
        let input = "MATCH (n) RETURN n";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_2() {
        let input = "MATCH (n:A) RETURN n";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_3() {
        let input = "MATCH (a:A:B) RETURN a";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_4() {
        let input = "MATCH (n {name: 'bar'}) RETURN n";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_5() {
        let input = "MATCH (n), (m) RETURN n.num AS n, m.num AS m";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_6() {
        let input = "MATCH (n $param) RETURN n";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_7() {
        let input = "MATCH ()-[r]-() RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }
    #[test]
    fn match_8() {
        let input = "MATCH ()-[r]->() RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_9() {
        let input = "MATCH ()<-[r]-() RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_10() {
        let input = "MATCH (), ()-[r]-() RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_11() {
        let input = "MATCH ()-[r]-(), () RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_12() {
        let input = "MATCH ()-[]-(), ()-[r]-() RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_13() {
        let input = "MATCH ()-[]-()-[r]-() RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_14() {
        let input = "MATCH ()-[]-()-[]-(), ()-[r]-() RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_15() {
        let input = "MATCH ()-[]-()-[]-(), ()-[r]-(), () RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_16() {
        let input = "MATCH ()-[]-()-[]-(), (), ()-[r]-() RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_17() {
        let input = "MATCH (x), (a)-[q]-(b), (s), (s)-[r]->(t)<-[]-(b) RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }
    #[test]
    fn match_18() {
        let input = "MATCH (:A)-[r]->(:B) RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_19() {
        let input = "MATCH ()-[r]-() RETURN type(r) AS r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_20() {
        let input = "MATCH (node)-[r:KNOWS {name: 'monkey'}]->(a) RETURN a";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_21() {
        let input = "MATCH (n)-[r:KNOWS|HATES]->(x) RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_22() {
        let input = "MATCH (a1)-[r:T]->() WITH r, a1 MATCH (a1)-[r:Y]->(b2) RETURN a1, r, b2";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_23() {
        let input = "MATCH ()-[r:FOO $param]->() RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    #[ignore = "pest parser fails this one, but it's a valid cypher query"]
    fn match_24() {
        let input = "MATCH (a)-[:ADMIN]-(b) WHERE a:A";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_25() {
        let input = "MATCH (n) WHERE n.name = 'Bar' RETURN n";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_26() {
        let input = "MATCH (n:Person)-->() WHERE n.name = 'Bob' RETURN n";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_27() {
        let input = "MATCH ()-[rel:X]-(a) WHERE a.name = 'Andres' RETURN a";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_28() {
        let input = "MATCH (advertiser)-[:ADV_HAS_PRODUCT]->(out)-[:AP_HAS_VALUE]->(red)<-[:AA_HAS_VALUE]-(a) WHERE advertiser.id = $1 AND a.id = $2 AND red.time > 12 AND out.name = 'product1' RETURN out.name";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }
}
