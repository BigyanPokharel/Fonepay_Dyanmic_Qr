<div align="center">

# Logo — Upload Specification

**Format requirements for any logo uploaded for the QR.**

`PNG only` · `Circular` · `White inside / Transparent outside` · `Black artwork`

</div>

---

## 1. Shape & Canvas

| Attribute | Requirement |
| :--- | :--- |
| **Shape** | The logo artwork must be a **perfect circle** |
| **Canvas** | Square, **1:1 aspect ratio**, with the circle centered inside it |
| **Dimensions** | **1024 × 1024 px** recommended · **512 × 512 px** minimum |

---

## 2. Fill Rules

| Zone | Requirement |
| :--- | :--- |
| **Inside the circle** | Solid **white** (`#FFFFFF`) background behind all artwork |
| **Outside the circle** | Fully **transparent** — the square canvas's corners must show no color |
| **Artwork / text / lines** | Solid **black** (`#000000`) only |

---

## 3. File Requirements

| Attribute | Requirement |
| :--- | :--- |
| **File format** | `.png` only — no `.jpg`, `.jpeg`, `.webp`, `.gif`, etc. |
| **Alpha channel** | Required (image must be **RGBA**, not RGB) |
| **Color mode** | Black & white only — no grayscale shading, no other colors |

---

## 4. Validation Checklist

- [ ] Corners of the canvas are transparent (alpha = 0)
- [ ] Area inside the circle is filled solid white
- [ ] All lines, text, and details are solid black
- [ ] File saved as `.png` with an alpha channel
- [ ] Minimum 512×512 px, ideally 1024×1024 px

