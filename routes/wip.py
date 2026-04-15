from datetime import datetime
from flask import Blueprint, render_template, request, jsonify
from sqlalchemy import func
from models.db import db, WIPChecklistItem, MeetingNote
from routes.notes import TITLE_MAX as NOTE_TITLE_MAX, BODY_MAX as NOTE_BODY_MAX

wip_bp = Blueprint("wip", __name__)

MAX_LEN = 500


def _clean_text(raw):
    if raw is None:
        return None
    if not isinstance(raw, str):
        return None
    return raw.strip()


def _validate_text(raw):
    text = _clean_text(raw)
    if not text:
        return None, (jsonify({"error": "text required"}), 400)
    if len(text) > MAX_LEN:
        return None, (jsonify({"error": "text too long"}), 400)
    return text, None


@wip_bp.route("/wip")
def index():
    open_items = (WIPChecklistItem.query
                  .filter_by(completed=False)
                  .order_by(WIPChecklistItem.sort_order.asc(),
                            WIPChecklistItem.id.asc())
                  .all())
    done_items = (WIPChecklistItem.query
                  .filter_by(completed=True)
                  .order_by(WIPChecklistItem.completed_at.desc(),
                            WIPChecklistItem.id.desc())
                  .all())
    notes = (MeetingNote.query
             .order_by(MeetingNote.updated_at.desc(), MeetingNote.id.desc())
             .all())
    return render_template("wip.html",
                           open_items=open_items,
                           done_items=done_items,
                           max_len=MAX_LEN,
                           notes=notes,
                           note_title_max=NOTE_TITLE_MAX,
                           note_body_max=NOTE_BODY_MAX)


@wip_bp.route("/wip/api/add", methods=["POST"])
def api_add():
    payload = request.get_json(silent=True) or {}
    text, err = _validate_text(payload.get("text"))
    if err:
        return err
    next_order = db.session.query(func.coalesce(func.max(WIPChecklistItem.sort_order), 0)).scalar() + 1
    item = WIPChecklistItem(text=text, completed=False, sort_order=next_order)
    db.session.add(item)
    db.session.commit()
    return jsonify(item.to_dict())


@wip_bp.route("/wip/api/set-completed", methods=["POST"])
def api_set_completed():
    payload = request.get_json(silent=True) or {}
    item_id = payload.get("id")
    completed = payload.get("completed")
    if not isinstance(completed, bool):
        return jsonify({"error": "completed (bool) required"}), 400
    item = WIPChecklistItem.query.get(item_id)
    if not item:
        return jsonify({"error": "not found"}), 404
    item.completed = completed
    item.completed_at = datetime.utcnow() if completed else None
    db.session.commit()
    return jsonify(item.to_dict())


@wip_bp.route("/wip/api/update", methods=["POST"])
def api_update():
    payload = request.get_json(silent=True) or {}
    item = WIPChecklistItem.query.get(payload.get("id"))
    if not item:
        return jsonify({"error": "not found"}), 404
    text, err = _validate_text(payload.get("text"))
    if err:
        return err
    item.text = text
    db.session.commit()
    return jsonify(item.to_dict())


@wip_bp.route("/wip/api/delete", methods=["POST"])
def api_delete():
    payload = request.get_json(silent=True) or {}
    item = WIPChecklistItem.query.get(payload.get("id"))
    if not item:
        return jsonify({"error": "not found"}), 404
    db.session.delete(item)
    db.session.commit()
    return jsonify({"ok": True})
