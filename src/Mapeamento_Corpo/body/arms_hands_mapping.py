import cv2
import mediapipe as mp
import numpy as np

mp_drawing = mp.solutions.drawing_utils
mp_styles = mp.solutions.drawing_styles
mp_holistic = mp.solutions.holistic
mp_face_mesh = mp.solutions.face_mesh


def calc_dist_xy(p1, p2, w, h):
    x1, y1 = int(p1.x * w), int(p1.y * h)
    x2, y2 = int(p2.x * w), int(p2.y * h)
    return np.hypot(x2 - x1, y2 - y1)


def analisar_movimentos(results, w, h):
    """
    Heurísticas simples de movimentos corporais:
    - mãos levantadas
    - braços abertos
    - braço cruzado/perto do peito
    """
    movimentos = []

    pose = results.pose_landmarks
    if pose is None:
        return ["sem corpo detectado"]

    lm = pose.landmark

    nariz = lm[mp_holistic.PoseLandmark.NOSE]
    ombro_esq = lm[mp_holistic.PoseLandmark.LEFT_SHOULDER]
    ombro_dir = lm[mp_holistic.PoseLandmark.RIGHT_SHOULDER]
    mao_esq = lm[mp_holistic.PoseLandmark.LEFT_WRIST]
    mao_dir = lm[mp_holistic.PoseLandmark.RIGHT_WRIST]

    # 1) Mãos levantadas (acima dos ombros)
    if mao_esq.y < ombro_esq.y:
        movimentos.append("mão esquerda levantada")
    if mao_dir.y < ombro_dir.y:
        movimentos.append("mão direita levantada")

    # 2) Braços abertos (mãos bem longe uma da outra)
    dist_maos = calc_dist_xy(mao_esq, mao_dir, w, h)
    if dist_maos > 0.6 * w:
        movimentos.append("braços abertos")

    # 3) Braço cruzado/perto do peito (mão perto do ombro oposto)
    dist_mao_esq_ombro_dir = calc_dist_xy(mao_esq, ombro_dir, w, h)
    dist_mao_dir_ombro_esq = calc_dist_xy(mao_dir, ombro_esq, w, h)

    if dist_mao_esq_ombro_dir < 0.25 * w:
        movimentos.append("braço esquerdo cruzado")
    if dist_mao_dir_ombro_esq < 0.25 * w:
        movimentos.append("braço direito cruzado")

    if not movimentos:
        movimentos.append("posição neutra")

    return movimentos


def main():
    cap = cv2.VideoCapture(0)
    if not cap.isOpened():
        print("Erro ao abrir webcam.")
        return

    with mp_holistic.Holistic(
        model_complexity=1,
        smooth_landmarks=True,
        enable_segmentation=False,
        min_detection_confidence=0.6,
        min_tracking_confidence=0.6,
    ) as holistic:

        while True:
            ret, frame = cap.read()
            if not ret:
                print("Erro ao capturar frame.")
                break

            h, w, _ = frame.shape

            rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            results = holistic.process(rgb)

            # POSE (corpo)
            if results.pose_landmarks:
                mp_drawing.draw_landmarks(
                    frame,
                    results.pose_landmarks,
                    mp_holistic.POSE_CONNECTIONS,
                    mp_drawing.DrawingSpec(
                        color=(80, 22, 10), thickness=2, circle_radius=4
                    ),
                    mp_drawing.DrawingSpec(
                        color=(80, 44, 121), thickness=2, circle_radius=2
                    ),
                )

            # MÃO ESQUERDA
            if results.left_hand_landmarks:
                mp_drawing.draw_landmarks(
                    frame,
                    results.left_hand_landmarks,
                    mp_holistic.HAND_CONNECTIONS,
                    mp_drawing.DrawingSpec(
                        color=(121, 22, 76), thickness=2, circle_radius=4
                    ),
                    mp_drawing.DrawingSpec(
                        color=(121, 44, 250), thickness=2, circle_radius=2
                    ),
                )

            # MÃO DIREITA
            if results.right_hand_landmarks:
                mp_drawing.draw_landmarks(
                    frame,
                    results.right_hand_landmarks,
                    mp_holistic.HAND_CONNECTIONS,
                    mp_drawing.DrawingSpec(
                        color=(245, 117, 66), thickness=2, circle_radius=4
                    ),
                    mp_drawing.DrawingSpec(
                        color=(245, 66, 230), thickness=2, circle_radius=2
                    ),
                )

            # ROSTO: usar conexões do Face Mesh
            if results.face_landmarks:
                mp_drawing.draw_landmarks(
                    frame,
                    results.face_landmarks,
                    mp_face_mesh.FACEMESH_TESSELATION,
                    mp_drawing.DrawingSpec(
                        color=(80, 110, 10), thickness=1, circle_radius=1
                    ),
                    mp_drawing.DrawingSpec(
                        color=(80, 256, 121), thickness=1, circle_radius=1
                    ),
                )

            # Analisar movimentos
            movimentos = analisar_movimentos(results, w, h)

            # Caixa de texto com movimentos detectados
            cv2.rectangle(frame, (0, 0), (420, 70), (0, 0, 0), -1)
            y0 = 25
            for mov in movimentos:
                cv2.putText(
                    frame,
                    mov,
                    (10, y0),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    0.6,
                    (0, 255, 255),
                    2,
                    cv2.LINE_AA,
                )
                y0 += 22

            cv2.imshow("Deteccao Avancada de Corpo, Bracos e Maos", frame)

            if cv2.waitKey(1) & 0xFF == ord("q"):
                break

    cap.release()
    cv2.destroyAllWindows()


if __name__ == "__main__":
    main()