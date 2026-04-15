from flask import Blueprint, request, jsonify
from models.db import db, MeetingNote

notes_bp = Blueprint("notes", __name__)

TITLE_MAX = 200
BODY_MAX = 50000


@notes_bp.route("/notes/api/list")
def api_list():
    notes = (MeetingNote.query
             .order_by(MeetingNote.updated_at.desc(), MeetingNote.id.desc())
             .all())
    return jsonify([n.to_dict() for n in notes])


@notes_bp.route("/notes/api/create", methods=["POST"])
def api_create():
    payload = request.get_json(silent=True) or {}
    title = (payload.get("title") or "").strip() or "Untitled"
    body = payload.get("body") or ""
    if len(title) > TITLE_MAX:
        return jsonify({"error": "title too long"}), 400
    if len(body) > BODY_MAX:
        return jsonify({"error": "body too long"}), 400
    note = MeetingNote(title=title, body=body)
    db.session.add(note)
    db.session.commit()
    return jsonify(note.to_dict())


@notes_bp.route("/notes/api/update", methods=["POST"])
def api_update():
    payload = request.get_json(silent=True) or {}
    note = MeetingNote.query.get(payload.get("id"))
    if not note:
        return jsonify({"error": "not found"}), 404
    if "title" in payload:
        title = (payload.get("title") or "").strip() or "Untitled"
        if len(title) > TITLE_MAX:
            return jsonify({"error": "title too long"}), 400
        note.title = title
    if "body" in payload:
        body = payload.get("body") or ""
        if len(body) > BODY_MAX:
            return jsonify({"error": "body too long"}), 400
        note.body = body
    db.session.commit()
    return jsonify(note.to_dict())


@notes_bp.route("/notes/api/delete", methods=["POST"])
def api_delete():
    payload = request.get_json(silent=True) or {}
    note = MeetingNote.query.get(payload.get("id"))
    if not note:
        return jsonify({"error": "not found"}), 404
    db.session.delete(note)
    db.session.commit()
    return jsonify({"ok": True})
