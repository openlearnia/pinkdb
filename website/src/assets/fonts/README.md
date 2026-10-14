# Vendored fonts

Self-hosted, OFL-licensed web fonts used by the pinkdb docs. No third-party
requests at runtime; every file below is committed to this repository.

| File | Family | Role | Upstream |
|---|---|---|---|
| `bricolage-grotesque.woff2` | Bricolage Grotesque (variable `wght` 400–800, `opsz` pinned at 96) | display / headings | https://fonts.google.com/specimen/Bricolage+Grotesque |
| `public-sans.woff2` | Public Sans (variable `wght` 100–900) | body text | https://fonts.google.com/specimen/Public+Sans |
| `public-sans-italic.woff2` | Public Sans Italic (variable `wght` 100–900) | body emphasis | idem |
| `jetbrains-mono.woff2` | JetBrains Mono (variable `wght` 400–700) | code, tables, data | https://fonts.google.com/specimen/JetBrains+Mono |

Licence texts: `OFL-Bricolage-Grotesque.txt`,
`OFL-Public-Sans.md`, `OFL-JetBrains-Mono.txt` (all SIL Open Font
License 1.1).

## Provenance

The `.woff2` files are the Google Fonts "latin" subset, then trimmed with
`fonttools` to keep only the glyphs this site needs — Basic Latin, Latin-1,
general punctuation, arrows, maths operators, box-drawing and geometric
shapes (the CLI transcripts are box-drawn):

```
pyftsubset <latin-subset>.woff2 \
    --unicodes="U+0020-007E,U+00A0-00FF,U+2010-2027,U+2030-205E,U+20AC,U+2122,U+2190-21FF,U+2200-22FF,U+2500-257F,U+25A0-25FF,U+FEFF,U+FFFD" \
    --layout-features='*' --flavor=woff2
fonttools varLib.instancer <subset>.woff2 opsz=96 wght=400:800   # display face
fonttools varLib.instancer <subset>.woff2 wght=400:700           # mono face
```

Total vendored bytes: **113,532 B** of fonts + **15,511 B** of licence
texts. The `.woff2` files are variable fonts, so headings can move between
Medium and ExtraBold without shipping extra files.
