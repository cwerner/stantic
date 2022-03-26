# 🚀 Getting started

This section shows you the core building blocks of `stantic` and you to get started quickly! Let's dive in...

(sta-scheme)=
:::{mermaid}
:caption: STA entity scheme. All entities are modeled as Pydantic classes in `stantic`

flowchart LR
    id1("Thing 🗼") -- has--> id2("Datastream 📈") -- has --> id3("Observation 🔢")
    id2 -.- id4("Sensor 🔬")
    id5("Location 🌍") -.- id1
    id6("ObservationProperty ⚙️") -.- id2
    id7("FeatureOfInterest 🧩") -.- id3
:::

