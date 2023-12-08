function OnUpdate(doc, meta) {
    doc["timestamp"] = Date.now()
    collection[meta.id]=doc;
}