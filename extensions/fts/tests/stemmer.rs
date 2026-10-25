//! The Porter stemmer against a reference word list, plus the Snowball
//! behaviour the reference cannot cover (`y` handling, short words).

use pinkdb_fts::porter::stem as porter;

/// A word list of a few thousand English words with their expected stems,
/// generated once from the Snowball `porter` implementation.
const FIXTURE: &str = include_str!("fixtures/porter_stems.tsv");

#[test]
fn matches_reference_stems() {
    let mut checked = 0;
    for line in FIXTURE.lines() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let (word, expected) = line.split_once('\t').expect("word<TAB>stem");
        assert_eq!(porter(word), expected, "stem of {word}");
        checked += 1;
    }
    assert!(checked >= 2_900, "only {checked} words checked");
}

/// Every rule step of the Porter algorithm, checked by hand against the
/// published algorithm (Porter 1980, appendix) and libstemmer's behaviour.
#[test]
fn rule_steps_by_hand() {
    for (input, expected) in [
        // step 1a
        ("caresses", "caress"),
        ("ponies", "poni"),
        ("ties", "ti"),
        ("caress", "caress"),
        ("cats", "cat"),
        // step 1b
        ("feed", "feed"),
        ("agreed", "agre"),
        ("plastered", "plaster"),
        ("bled", "bled"),
        ("motoring", "motor"),
        ("sing", "sing"),
        ("conflated", "conflat"),
        ("troubled", "troubl"),
        ("sized", "size"),
        ("hopping", "hop"),
        ("tanned", "tan"),
        ("falling", "fall"),
        ("hissing", "hiss"),
        ("fizzed", "fizz"),
        ("failing", "fail"),
        ("filing", "file"),
        // step 1c
        ("happy", "happi"),
        ("sky", "sky"),
        // step 2
        ("relational", "relat"),
        ("conditional", "condit"),
        ("rational", "ration"),
        ("valenci", "valenc"),
        ("hesitanci", "hesit"),
        ("digitizer", "digit"),
        ("conformabli", "conform"),
        ("radicalli", "radic"),
        ("differentli", "differ"),
        ("vileli", "vile"),
        ("analogousli", "analog"),
        ("vietnamization", "vietnam"),
        ("predication", "predic"),
        ("operator", "oper"),
        ("feudalism", "feudal"),
        ("decisiveness", "decis"),
        ("hopefulness", "hope"),
        ("callousness", "callous"),
        ("formaliti", "formal"),
        ("sensitiviti", "sensit"),
        ("sensibiliti", "sensibl"),
        // step 3
        ("triplicate", "triplic"),
        ("formative", "form"),
        ("formalize", "formal"),
        ("electriciti", "electr"),
        ("electrical", "electr"),
        ("hopeful", "hope"),
        ("goodness", "good"),
        // step 4
        ("revival", "reviv"),
        ("allowance", "allow"),
        ("inference", "infer"),
        ("airliner", "airlin"),
        ("gyroscopic", "gyroscop"),
        ("adjustable", "adjust"),
        ("defensible", "defens"),
        ("irritant", "irrit"),
        ("replacement", "replac"),
        ("adjustment", "adjust"),
        ("dependent", "depend"),
        ("adoption", "adopt"),
        ("homologou", "homolog"),
        ("communism", "commun"),
        ("activate", "activ"),
        ("angulariti", "angular"),
        ("homologous", "homolog"),
        ("effective", "effect"),
        ("bowdlerize", "bowdler"),
        // step 5
        ("probate", "probat"),
        ("rate", "rate"),
        ("cease", "ceas"),
        ("controll", "control"),
        ("roll", "roll"),
        // Snowball's doubled-consonant test keeps `l`, `s` and `z`
        // undoubled, and only a "short syllable" blocks step 1b's `e` drop.
        ("kissing", "kiss"),
        ("buzzing", "buzz"),
        ("fizzing", "fizz"),
        ("sniffing", "snif"),
        ("dying", "dy"),
        ("crying", "cry"),
        ("joy", "joi"),
        ("news", "new"),
        // Snowball's porter differs from the paper here: `bli` is not a
        // step 2 suffix (only `abli` is), and `logi` is not a suffix at all.
        ("accessibly", "accessibli"),
        ("abiology", "abiologi"),
        ("appall", "appal"),
    ] {
        assert_eq!(porter(input), expected, "stem of {input}");
    }
}
