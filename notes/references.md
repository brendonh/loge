- Object has named sets of links
  K -> [...]

- Links accessed via transaction, not on object
  t.ReadLinks(obj, "children")

- Links always lists internally, convenience functions for singles
  t.ReadLink -- panic if > 1
  t.WriteLink -- sets to [v]

- Out links have predeclared type


CreateType(
        "foo",
        &Foo{},
        { "parent": "foo",
          "children": "foo",
          "category": "cat" })

t.SetLink("foo", "parent", "bar")
t.SetLinks("foo", "parent", ["bar"])

t.AddLink("foo", "children", "wib")
t.RemoveLink("foo", "children", "wib")

t.ReadLink("foo", "parent")
t.ReadLinks("foo", "children")
t.HasLink("foo", "children", "wib")

- Create foo:
  - Initialize links map from type:
    { "parent": LinkSet(),
      "children": LinkSet(), 
      "category": LinkSet() }
  - Naive LinkSet impl is map[string]bool
  - Replace with b-tree in storage later

- Transactions:
    
    